"""
Basic test for Supabase Broker initialization
"""

import os
import pytest
import psycopg
import sys

# Add dramatiq to path
sys.path.insert(0, os.path.abspath('/Users/ankurpansari/Documents/GitHub/dramatiq'))

# Import the broker
from dramatiq.brokers.supabase import SupabaseBroker

def test_import():
    """Test that the module can be imported."""
    assert SupabaseBroker is not None
    
    # Print info for debugging
    print(f"psycopg version: {psycopg.__version__}")
    print(f"Module location: {psycopg.__file__}")

if __name__ == "__main__":
    test_import()
    print("Import test passed.")