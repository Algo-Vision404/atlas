import os
import subprocess
import sys
import time


def run_service(name: str, path: str, mock_mode: str = "true") -> subprocess.Popen:
    print(f"Starting {name}...")
    env = os.environ.copy()
    env["PYTHONPATH"] = "."
    env.setdefault("MOCK_MODE", mock_mode)
    # Deliberately inherit stdout/stderr: piping without draining the buffers
    # deadlocks the child as soon as it logs more than the pipe capacity.
    return subprocess.Popen([sys.executable, path], env=env)


def stop_all(processes: list[subprocess.Popen]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
    deadline = time.time() + 10
    for process in processes:
        if process.poll() is None:
            try:
                process.wait(timeout=max(0.1, deadline - time.time()))
            except subprocess.TimeoutExpired:
                process.kill()


if __name__ == "__main__":
    services = [
        ("Search API", "apps/search_api/main.py"),
        ("Crawl API", "apps/crawler/api.py"),
        ("Crawler", "apps/crawler/engine.py"),
        ("Indexer", "apps/indexer/worker.py"),
    ]

    processes: list[subprocess.Popen] = []
    exit_code = 0
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
            for process in processes:
                if process.poll() is not None:
                    print(f"Process {process.pid} exited with code {process.returncode}")
                    exit_code = process.returncode or 1
                    raise KeyboardInterrupt
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nStopping ATLAS services...")
    finally:
        stop_all(processes)
        print("Done.")
    sys.exit(exit_code)
