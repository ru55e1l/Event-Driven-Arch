import threading
import importlib
import sys
import os

def run_consumer(module_name):
    try:
        module = importlib.import_module(module_name)
        
        start_function = None
        for attr_name in dir(module):
            if attr_name.startswith('start_') and attr_name.endswith('_consumer'):
                start_function = getattr(module, attr_name)
                break
        
        if start_function:
            print(f"starting {module_name}...")
            start_function()
    except Exception as e:
        print(f"error running {module_name}: {str(e)}")

def main():
    consumer_files = [f[:-3] for f in os.listdir('.') 
                     if f.startswith('consumer_') and f.endswith('.py')]
    
    
    threads = []
    for consumer in consumer_files:
        thread = threading.Thread(target=run_consumer, args=(consumer,))
        thread.daemon = True 
        threads.append(thread)
        thread.start()
    
    print("\nall consumers are running. Press Ctrl+C to stop all services.\n")
    
    try:
        while True:
            for thread in threads:
                thread.join(1) 
    except KeyboardInterrupt:
        print("\nshutting down all consumers...")
        sys.exit(0)

if __name__ == "__main__":
    main() 