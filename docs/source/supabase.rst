.. _supabase-broker:

Supabase Queue Broker
====================

The Supabase Queue broker uses `Supabase's Queue <https://supabase.com/docs/guides/queues/api>`_
functionality (which is built on PostgreSQL) to power Dramatiq.

.. warning::
   
   The Supabase Queue broker requires the ``psycopg`` package.
   Install it with ``pip install psycopg``.


Broker Options
-------------

The Supabase Queue broker takes the following options:

.. py:class:: SupabaseBroker(\*, host="localhost", port=5432, database="postgres", user="postgres", password="postgres", middleware=None, visibility_timeout=30, namespace="dramatiq", min_conn=1, max_conn=10, **options)

   :param str host: The PostgreSQL host to connect to.
   :param int port: The port the PostgreSQL server is running on.
   :param str database: The database to use.
   :param str user: The database user to connect with.
   :param str password: The database password to connect with.
   :param list middleware: The list of middleware to use with this broker.
   :param int visibility_timeout: The visibility timeout for messages, in seconds.
   :param str namespace: A prefix to use for all queue names.
   :param int min_conn: The minimum number of connections to keep in the pool.
   :param int max_conn: The maximum number of connections to keep in the pool.
   :param dict options: Additional parameters to pass to ``psycopg.connect``.


Example Usage
------------

Here's a minimal example of how you might use the Supabase Queue broker:

.. code-block:: python

    import dramatiq
    from dramatiq.brokers.supabase import SupabaseBroker

    broker = SupabaseBroker(
        host="your-project.supabase.co",
        port=5432,
        database="postgres",
        user="postgres",
        password="your-password",
        namespace="my_app"
    )
    dramatiq.set_broker(broker)

    @dramatiq.actor
    def add(a, b):
        return a + b

    # Enqueue a message
    add.send(1, 2)


Using with Supabase
------------------

When using with Supabase, you'll need to configure the broker with your Supabase connection details:

1. Get your database connection details from the Supabase dashboard
2. Configure the broker with these credentials
3. Make sure you have the Queue functionality enabled in your project

.. note::

   Supabase Queues requires the ``pgmq_public`` schema, which should be automatically
   available in projects with Queue functionality enabled.


Implementation Notes
------------------

* Messages are stored in JSON format in PostgreSQL
* The broker uses a connection pool for efficient database access
* Queue names are prefixed with the namespace parameter to avoid conflicts
* Visibility timeout is used to ensure at-most-once delivery guarantees
* Delayed messages are implemented using Supabase Queue's built-in delay mechanism


Example Application
------------------

A complete example using the Supabase Queue broker can be found in the examples directory:

.. literalinclude:: ../../examples/supabase_example.py
   :language: python