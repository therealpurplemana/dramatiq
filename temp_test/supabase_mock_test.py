"""
Mock test for Supabase Broker 
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Add dramatiq to path
sys.path.insert(0, os.path.abspath('/Users/ankurpansari/Documents/GitHub/dramatiq'))

# Import the broker
from dramatiq.brokers.supabase import SupabaseBroker

class MockCursor:
    def __init__(self):
        self.description = [("column",)]
    
    def execute(self, *args, **kwargs):
        pass
    
    def fetchone(self):
        return [1]
    
    def fetchall(self):
        return []

class MockConnection:
    def __init__(self):
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        pass
    
    def cursor(self):
        return MockCursor()
    
    def commit(self):
        pass

class TestSupabaseBroker(unittest.TestCase):
    @patch('psycopg.pool.ConnectionPool')
    def test_broker_initialization(self, mock_pool_class):
        """Test that the broker can be initialized with mocked connections."""
        # Setup the mock connection pool
        mock_pool = MagicMock()
        mock_pool.getconn.return_value = MockConnection()
        mock_pool_class.return_value = mock_pool
        
        # Create the broker
        broker = SupabaseBroker(namespace="dramatiq_test")
        self.assertEqual(broker.namespace, "dramatiq_test")
        
        # Test declaring a queue
        broker.declare_queue("test_queue")
        self.assertIn("test_queue", broker.queues)
        
        # Test cleanup
        broker.flush_all()
        broker.close()
        
if __name__ == "__main__":
    unittest.main()