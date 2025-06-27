import os
import threading
import subprocess
import time

def execute_command(command, name):
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(f"{result.stdout.strip()}")
        if result.stderr:
            print(f"[{name} ERROR]\n{result.stderr.strip()}")
    except Exception as e:
        print(f"[{name} EXCEPTION] {e}")

def threaded_subprocess_test():
    commands = {
        "Subprocess 1": "sleep 8 && echo Subprocess 1",
        "Subprocess 2": "sleep 2 && echo Subprocess 2",
        "Subprocess 3": "sleep 4 && echo Subprocess 3",
    }

    threads = []
    start_time = time.time()

    for name, cmd in commands.items():
        thread = threading.Thread(target=execute_command, args=(cmd, name))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    end_time = time.time()
    print(f"\nTotal time: {end_time - start_time:.2f} seconds")

# Run the threaded test
threaded_subprocess_test()
