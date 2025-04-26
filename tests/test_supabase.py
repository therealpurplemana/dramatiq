import os
import pytest

from dramatiq import Worker
from dramatiq.brokers.supabase import SupabaseBroker

# Skip these tests if psycopg is not available
pytest.importorskip("psycopg")

# Skip these tests if environment variables indicate we should
if not os.getenv("SUPABASE_TEST", ""):
    pytestmark = pytest.mark.skip("Set SUPABASE_TEST=1 to run the Supabase broker tests")


def test_supabase_broker():
    """Basic test for the Supabase broker initialization."""
    # Configure with default settings
    broker = SupabaseBroker(namespace="dramatiq_test")
    
    # Test declaring a queue
    broker.declare_queue("test_queue")
    
    # Test it was added to broker queues
    assert "test_queue" in broker.queues
    
    # Clean up
    broker.flush_all()
    broker.close()


def test_supabase_worker():
    """Test that the worker can be initialized with a Supabase broker."""
    broker = SupabaseBroker(namespace="dramatiq_test")
    
    # Create a worker
    worker = Worker(broker)
    
    # Start the worker
    worker.start()
    
    # Clean up
    worker.stop()
    broker.close()