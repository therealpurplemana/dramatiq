"""
Example for using Dramatiq with Supabase Queue.

To run this example:
1. Make sure you have a Supabase project with Queue functionality enabled
2. Set the environment variables:
   - SUPABASE_HOST (default: localhost)
   - SUPABASE_PORT (default: 5432)
   - SUPABASE_USER (default: postgres)
   - SUPABASE_PASSWORD (default: postgres)
   - SUPABASE_DATABASE (default: postgres)

3. Run the worker:
   python examples/supabase_example.py worker

4. In another terminal, run the client:
   python examples/supabase_example.py send "https://example.com"
"""

import os
import sys
import time
import requests
import dramatiq

# Import the Supabase broker
from dramatiq.brokers.supabase import SupabaseBroker

# Get connection details from environment
supabase_host = os.getenv("SUPABASE_HOST", "localhost")
supabase_port = int(os.getenv("SUPABASE_PORT", "5432"))
supabase_user = os.getenv("SUPABASE_USER", "postgres")
supabase_password = os.getenv("SUPABASE_PASSWORD", "postgres")
supabase_database = os.getenv("SUPABASE_DATABASE", "postgres")

# Create the broker
broker = SupabaseBroker(
    host=supabase_host,
    port=supabase_port,
    user=supabase_user,
    password=supabase_password,
    database=supabase_database,
    namespace="dramatiq_example"
)

# Tell dramatiq to use this broker
dramatiq.set_broker(broker)


# Define an actor
@dramatiq.actor(max_retries=3)
def count_words(url):
    """Count words on a web page."""
    print(f"Counting words at {url}...")
    response = requests.get(url)
    count = len(response.text.split())
    print(f"There are {count} words at {url!r}.")
    return count


@dramatiq.actor
def add(a, b):
    """Add two numbers together."""
    print(f"Adding {a} + {b}...")
    time.sleep(1)  # Simulate work
    result = a + b
    print(f"Result: {result}")
    return result


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} [worker|send URL|add NUM NUM]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "worker":
        # Start a worker
        from dramatiq.cli import main
        sys.exit(main(["dramatiq", "examples.supabase_example"]))
    
    elif command == "send":
        if len(sys.argv) < 3:
            print(f"Usage: {sys.argv[0]} send URL")
            sys.exit(1)
        
        url = sys.argv[2]
        # Send a message
        message = count_words.send(url)
        print(f"Enqueued task {message.message_id} to count words at {url}")
    
    elif command == "add":
        if len(sys.argv) < 4:
            print(f"Usage: {sys.argv[0]} add NUM NUM")
            sys.exit(1)
        
        try:
            a = int(sys.argv[2])
            b = int(sys.argv[3])
        except ValueError:
            print("Both arguments must be numbers")
            sys.exit(1)
        
        # Send a message
        message = add.send(a, b)
        print(f"Enqueued task {message.message_id} to add {a} + {b}")
    
    elif command == "add-delayed":
        if len(sys.argv) < 4:
            print(f"Usage: {sys.argv[0]} add-delayed NUM NUM")
            sys.exit(1)
        
        try:
            a = int(sys.argv[2])
            b = int(sys.argv[3])
        except ValueError:
            print("Both arguments must be numbers")
            sys.exit(1)
        
        # Send a message with a delay
        message = add.send_with_options(args=(a, b), delay=5000)  # 5 second delay
        print(f"Enqueued task {message.message_id} to add {a} + {b} with a 5-second delay")
    
    else:
        print(f"Unknown command: {command}")
        print(f"Usage: {sys.argv[0]} [worker|send URL|add NUM NUM|add-delayed NUM NUM]")
        sys.exit(1)