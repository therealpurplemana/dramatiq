"""Supabase Queue broker implementation for Dramatiq."""

import time
import uuid
import json
from threading import Lock
from typing import Dict, List, Optional, Set, Any, Union

try:
    import psycopg
    from psycopg.pool import ConnectionPool
except ImportError:  # pragma: no cover
    psycopg = None

from ..broker import Broker, Consumer, MessageProxy
from ..common import compute_backoff, current_millis, dq_name
from ..errors import ConnectionClosed, QueueJoinTimeout
from ..logging import get_logger
from ..message import Message

#: The amount of time in milliseconds that dead-lettered messages are
#: kept in archive for.
DEFAULT_DEAD_MESSAGE_TTL = 86400000 * 7  # 7 days

#: The amount of time in seconds for message visibility timeout.
DEFAULT_VISIBILITY_TIMEOUT = 30


class SupabaseBroker(Broker):
    """A broker that can be used with Supabase Queues.

    Parameters:
      host(str): The PostgreSQL host. Default: "localhost".
      port(int): The PostgreSQL port. Default: 5432.
      database(str): PostgreSQL database name. Default: "postgres".
      user(str): PostgreSQL username. Default: "postgres".
      password(str): PostgreSQL password. Default: "postgres".
      middleware(list[Middleware]): The middleware list.
      visibility_timeout(int): The visibility timeout in seconds. Default: 30.
      namespace(str): Optional namespace for queue names. Default: "dramatiq".
      min_conn(int): Minimum number of connections in the pool. Default: 1.
      max_conn(int): Maximum number of connections in the pool. Default: 10.
      **options: Additional keyword arguments passed to psycopg.connect().
    """

    def __init__(
        self, *,
        host="localhost",
        port=5432,
        database="postgres",
        user="postgres",
        password="postgres",
        middleware=None,
        visibility_timeout=DEFAULT_VISIBILITY_TIMEOUT,
        namespace="dramatiq",
        min_conn=1,
        max_conn=10,
        **options
    ):
        if psycopg is None:  # pragma: no cover
            raise ImportError(
                "You need to install psycopg in order to use the Supabase broker."
            ) from None

        super().__init__(middleware=middleware)

        self.broker_id = str(uuid.uuid4())
        self.namespace = namespace
        self.visibility_timeout = visibility_timeout
        self.queues = set()
        self.delay_queues = set()
        self.pool_lock = Lock()
        self._connection_pool = None
        
        # Connection parameters
        self.connection_params = {
            "host": host,
            "port": port,
            "dbname": database,
            "user": user,
            "password": password,
            **options
        }
        
        self.min_conn = min_conn
        self.max_conn = max_conn
        
        # Create connection pool when broker is initialized
        self._ensure_connection_pool()
        
        # Ensure the pgmq_public schema is available
        self._ensure_schema()

    def _ensure_connection_pool(self):
        """Ensure the connection pool is initialized."""
        if self._connection_pool is None:
            with self.pool_lock:
                if self._connection_pool is None:
                    self.logger.debug("Creating connection pool...")
                    self._connection_pool = ConnectionPool(
                        psycopg.connect,
                        min_size=self.min_conn,
                        max_size=self.max_conn,
                        **self.connection_params
                    )
                    
    def _ensure_schema(self):
        """Ensure the pgmq_public schema exists."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq_public'")
            if not cursor.fetchone():
                self.logger.warning("pgmq_public schema not found. Supabase Queue functionality may not be available.")

    def _get_connection(self):
        """Get a connection from the pool, recreating it if necessary."""
        self._ensure_connection_pool()
        
        try:
            return self._connection_pool.getconn()
        except Exception as e:
            self.logger.error("Failed to get connection: %s", e)
            # Try to recreate the pool and retry
            self._connection_pool = None
            self._ensure_connection_pool()
            return self._connection_pool.getconn()
            
    def _release_connection(self, conn):
        """Return a connection to the pool."""
        if self._connection_pool is not None:
            self._connection_pool.putconn(conn)

    def _queue_name(self, queue_name):
        """Get the queue name with namespace applied."""
        return f"{self.namespace}_{queue_name}"

    @property
    def consumer_class(self):
        """Return the consumer class for this broker."""
        return _SupabaseConsumer

    def close(self):
        """Close the broker connection to Supabase."""
        if self._connection_pool is not None:
            self.logger.debug("Closing connection pool...")
            self._connection_pool.close()
            self._connection_pool = None

    def consume(self, queue_name, prefetch=1, timeout=5000):
        """Create a consumer for the given queue.

        Parameters:
          queue_name(str): The queue to consume from.
          prefetch(int): Number of messages to prefetch.
          timeout(int): Timeout in milliseconds.

        Returns:
          Consumer: A new consumer.
        """
        return self.consumer_class(self, queue_name, prefetch, timeout)

    def declare_queue(self, queue_name):
        """Declare a queue in Supabase.

        Parameters:
          queue_name(str): The name of the queue to create.
        """
        if queue_name in self.queues:
            return

        self.emit_before("declare_queue", queue_name)
        
        queue_name_with_ns = self._queue_name(queue_name)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            self.logger.debug("Declaring queue: %s", queue_name_with_ns)
            
            # Create the queue if it doesn't exist
            cursor.execute(
                "SELECT pgmq_public.create_queue(%s)",
                (queue_name_with_ns,)
            )
            conn.commit()
            
        self.queues.add(queue_name)
        self.emit_after("declare_queue", queue_name)

        # Declare the delay queue
        delayed_name = dq_name(queue_name)
        if delayed_name not in self.delay_queues:
            delayed_name_with_ns = self._queue_name(delayed_name)
            with self._get_connection() as conn:
                cursor = conn.cursor()
                self.logger.debug("Declaring delay queue: %s", delayed_name_with_ns)
                
                # Create the delay queue if it doesn't exist
                cursor.execute(
                    "SELECT pgmq_public.create_queue(%s)",
                    (delayed_name_with_ns,)
                )
                conn.commit()
                
            self.delay_queues.add(delayed_name)
            self.emit_after("declare_delay_queue", delayed_name)

    def enqueue(self, message, *, delay=None):
        """Enqueue a message.

        Parameters:
          message(Message): The message to enqueue.
          delay(int): The minimum amount of time, in milliseconds, to delay the message by.

        Returns:
          Message: The enqueued message.
        """
        queue_name = message.queue_name
        
        # Each enqueued message must have a unique id
        message = message.copy(options={
            "supabase_message_id": str(uuid.uuid4()),
        })

        if delay is not None:
            delay_seconds = delay / 1000.0  # Convert from ms to seconds
            queue_name = dq_name(queue_name)
            message = message.copy(
                queue_name=queue_name,
                options={
                    "eta": current_millis() + delay,
                },
            )

        self.logger.debug("Enqueueing message %r on queue %r.", message.message_id, queue_name)
        self.emit_before("enqueue", message, delay)
        
        queue_name_with_ns = self._queue_name(queue_name)
        encoded_message = message.encode()
        
        # Convert the message to a format suitable for Supabase Queue
        message_json = {
            "id": message.message_id,
            "data": encoded_message.decode('utf-8')
        }
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            delay_seconds_param = delay_seconds if delay is not None and delay > 0 else 0
            
            # Send message to queue
            cursor.execute(
                "SELECT pgmq_public.send(%s, %s, %s)",
                (queue_name_with_ns, json.dumps(message_json), delay_seconds_param)
            )
            
            result = cursor.fetchone()
            if result:
                # Store the message ID returned by Supabase
                message = message.copy(options={
                    "supabase_message_id": result[0],
                })
                
            conn.commit()
            
        self.emit_after("enqueue", message, delay)
        return message

    def flush(self, queue_name):
        """Drop all the messages from a queue.

        Parameters:
          queue_name(str): The queue to flush.
        """
        for name in (queue_name, dq_name(queue_name)):
            queue_name_with_ns = self._queue_name(name)
            with self._get_connection() as conn:
                cursor = conn.cursor()
                # First ensure the queue exists
                cursor.execute(
                    "SELECT pgmq_public.create_queue(%s)",
                    (queue_name_with_ns,)
                )
                
                # Now purge all messages
                # Supabase Queues doesn't have a direct purge function,
                # so we'll pop messages until the queue is empty
                while True:
                    cursor.execute(
                        "SELECT pgmq_public.pop(%s, 100)",
                        (queue_name_with_ns,)
                    )
                    result = cursor.fetchall()
                    if not result or len(result) == 0:
                        break
                
                conn.commit()

    def flush_all(self):
        """Drop all messages from all declared queues."""
        for queue_name in self.queues:
            self.flush(queue_name)

    def join(self, queue_name, *, timeout=None):
        """Wait for all the messages on the given queue to be processed.

        Parameters:
          queue_name(str): The queue to wait on.
          timeout(int): The max amount of time, in milliseconds, to wait.

        Raises:
          QueueJoinTimeout: When the timeout elapses.
        """
        deadline = timeout and time.monotonic() + timeout / 1000
        
        while True:
            if deadline and time.monotonic() >= deadline:
                raise QueueJoinTimeout(queue_name)
            
            queue_name_with_ns = self._queue_name(queue_name)
            with self._get_connection() as conn:
                cursor = conn.cursor()
                # Check if there are any messages in the queue
                cursor.execute(
                    "SELECT COUNT(*) FROM pgmq_public.get_queue(%s) WHERE visible_at <= NOW()",
                    (queue_name_with_ns,)
                )
                count = cursor.fetchone()[0]
                
                if count == 0:
                    return
            
            time.sleep(0.1)


class _SupabaseConsumer(Consumer):
    """Supabase Queue Consumer implementation for Dramatiq."""

    def __init__(self, broker, queue_name, prefetch, timeout):
        self.logger = get_logger(__name__, type(self))
        self.broker = broker
        self.queue_name = queue_name
        self.prefetch = prefetch
        self.timeout = timeout
        
        self.queue_name_with_ns = broker._queue_name(queue_name)
        self.message_cache = []
        self.in_flight_messages = {}
        self.misses = 0

    @property
    def outstanding_message_count(self):
        """Return the number of messages currently being processed."""
        return len(self.in_flight_messages) + len(self.message_cache)

    def ack(self, message):
        """Acknowledge that a message has been processed.

        Parameters:
          message(MessageProxy): The message being acknowledged.
        """
        try:
            # Delete the message from Supabase Queue
            if "supabase_message_id" in message.options:
                msg_id = message.options["supabase_message_id"]
                with self.broker._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT pgmq_public.delete(%s, %s)",
                        (self.queue_name_with_ns, msg_id)
                    )
                    conn.commit()
                    
                self.in_flight_messages.pop(message.message_id, None)
        except Exception as e:
            self.logger.error("Failed to acknowledge message: %s", e)
            raise ConnectionClosed(e) from None

    def nack(self, message):
        """Move a message to the dead-letter queue (archive in Supabase).

        Parameters:
          message(MessageProxy): The message being rejected.
        """
        try:
            if "supabase_message_id" in message.options:
                msg_id = message.options["supabase_message_id"]
                with self.broker._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT pgmq_public.archive(%s, %s)",
                        (self.queue_name_with_ns, msg_id)
                    )
                    conn.commit()
                    
                self.in_flight_messages.pop(message.message_id, None)
        except Exception as e:
            self.logger.error("Failed to reject message: %s", e)
            raise ConnectionClosed(e) from None

    def requeue(self, messages):
        """Requeue unacked messages back to the queue.

        Parameters:
          messages(list[MessageProxy]): The messages to requeue.
        """
        if not messages:
            return
            
        message_ids = []
        for message in messages:
            if "supabase_message_id" in message.options:
                message_ids.append(message.options["supabase_message_id"])
                
        if not message_ids:
            return
            
        self.logger.debug("Re-enqueueing %r on queue %r.", message_ids, self.queue_name)
        
        # In Supabase Queues, messages automatically become visible again 
        # after the visibility timeout expires. There's no direct requeue operation.
        # However, we could force this by updating the visibility_timeout in the 
        # underlying queue table, but this is an internal implementation detail
        # and may not be supported by Supabase.
        #
        # For now, just log the requeue request and rely on the visibility timeout.
        self.logger.info(
            "Explicit requeue not supported in Supabase Queues. Messages will become visible "
            "again after visibility timeout expires. Message IDs: %r", message_ids
        )

    def __next__(self):
        """Get the next message off of the queue.

        Returns:
          MessageProxy: A transparent proxy around a Message.
        """
        try:
            while True:
                try:
                    # Try the fast path first - getting from cache
                    data = self.message_cache.pop(0)
                    self.misses = 0
                    
                    # Deserialize the message
                    dramatiq_message = Message.decode(data["data"].encode("utf-8"))
                    
                    # Store the Supabase message ID for later acknowledgment
                    dramatiq_message = dramatiq_message.copy(options={
                        "supabase_message_id": data["msg_id"],
                    })
                    
                    # Track in-flight messages
                    self.in_flight_messages[dramatiq_message.message_id] = dramatiq_message
                    
                    return MessageProxy(dramatiq_message)
                except IndexError:
                    # If there are fewer messages currently being processed than
                    # we're allowed to prefetch, get up to that number of messages
                    messages = []
                    if self.outstanding_message_count < self.prefetch:
                        try:
                            with self.broker._get_connection() as conn:
                                cursor = conn.cursor()
                                cursor.execute(
                                    "SELECT * FROM pgmq_public.read(%s, %s, %s)",
                                    (
                                        self.queue_name_with_ns, 
                                        self.broker.visibility_timeout,
                                        self.prefetch - self.outstanding_message_count
                                    )
                                )
                                results = cursor.fetchall()
                                if results:
                                    # Process results into usable messages
                                    columns = [desc[0] for desc in cursor.description]
                                    for row in results:
                                        message_data = dict(zip(columns, row))
                                        try:
                                            # Parse the message JSON
                                            message_json = json.loads(message_data["message"])
                                            messages.append({
                                                "msg_id": message_data["msg_id"],
                                                "data": message_json["data"]
                                            })
                                        except (json.JSONDecodeError, KeyError) as e:
                                            self.logger.error("Failed to parse message payload: %s", e)
                        except Exception as e:
                            self.logger.error("Failed to read messages: %s", e)
                            raise ConnectionClosed(e) from None
                            
                        if messages:
                            self.message_cache = messages
                    
                    # Because we didn't get any messages, we should
                    # progressively long poll up to the idle timeout
                    if not messages:
                        self.misses, backoff_ms = compute_backoff(self.misses, max_backoff=self.timeout)
                        time.sleep(backoff_ms / 1000)
                        return None
        except Exception as e:
            self.logger.error("Unexpected error in consumer: %s", e)
            raise ConnectionClosed(e) from None