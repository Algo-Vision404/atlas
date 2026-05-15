import subprocess
import sys
import os
import time

def run_service(name, path):
    print(f"Starting {name}...")
    env = os.environ.copy()
    env["PYTHONPATH"] = "."
    process = subprocess.Popen(
        [sys.executable, path],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    return process

if __name__ == "__main__":
    services = [
        ("Search API", "apps/search_api/main.py"),
        ("Crawler", "apps/crawler/engine.py"),
        ("Indexer", "apps/indexer/engine.py")
    ]
    
    processes = []
    try:
        for name, path in services:
            processes.append(run_service(name, path))
            time.sleep(1) # Give it a second to start
            
        print("\nAll ATLAS services are running in MOCK MODE.")
        print("Press Ctrl+C to stop all services.\n")
        
        while True:
            for p in processes:
                # Check if any process died
                if p.poll() is not None:
                    print(f"Process {p.pid} exited with code {p.returncode}")
                    sys.exit(1)
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nStopping ATLAS services...")
        for p in processes:
            p.terminate()
        print("Done.")
