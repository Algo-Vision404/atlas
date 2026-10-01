import os
import subprocess
import sys
import time


def run_service(name: str, path: str, mock_mode: str = "true") -> subprocess.Popen:
    print(f"Starting {name}...")
    env = os.environ.copy()
    env["PYTHONPATH"] = "."
    env.setdefault("MOCK_MODE", mock_mode)
    process = subprocess.Popen(
        [sys.executable, path],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return process


if __name__ == "__main__":
    services = [
        ("Search API", "apps/search_api/main.py"),
        ("Crawl API", "apps/crawler/api.py"),
        ("Crawler", "apps/crawler/engine.py"),
        ("Indexer", "apps/indexer/worker.py"),
    ]

    processes = []
    try:
        for name, path in services:
            processes.append(run_service(name, path))
            time.sleep(1)

        mode_label = os.environ.get("MOCK_MODE", "true").lower()
        if mode_label in {"1", "true", "yes"}:
            print("\nAll ATLAS services are running in MOCK MODE.")
        else:
            print("\nAll ATLAS services are running.")
        print("Press Ctrl+C to stop all services.\n")

        while True:
            for p in processes:
                if p.poll() is not None:
                    output = p.stdout.read() if p.stdout else ""
                    print(f"Process {p.pid} exited with code {p.returncode}")
                    if output:
                        print(output)
                    sys.exit(1)
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nStopping ATLAS services...")
        for p in processes:
            p.terminate()
        print("Done.")
