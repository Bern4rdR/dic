import json
import time
import uuid
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime


# Directory where logs will be stored
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


# Create a unique ID for this execution
run_id = uuid.uuid4().hex[:8]

# Create the log file for this run
start_time = datetime.now()

log_file = LOG_DIR / (
    f"run_{start_time.strftime('%Y%m%d_%H%M%S')}_{run_id}.json"
)

# The complete log for this run
run_log = {
    "run_id": run_id,
    "started_at": start_time.isoformat(timespec="seconds"),
    "steps": []
}


def save_log():
    """Save the current state of the run log."""
    with log_file.open("w", encoding="utf-8") as f:
        json.dump(run_log, f, indent=2)


@contextmanager
def log_step(name, info=None):
    start = datetime.now()
    timer = time.perf_counter()

    step = {
        "name": name,
        "started_at": start.isoformat(timespec="seconds"),
        "ended_at": None,
        "duration_seconds": None,
        "status": "running",
        "info": info or {}
    }

    run_log["steps"].append(step)
    save_log()

    try:
        yield step["info"]

    except Exception:
        step["ended_at"] = datetime.now().isoformat(timespec="seconds")
        step["duration_seconds"] = round(
            time.perf_counter() - timer, 5
        )
        step["status"] = "failed"
        save_log()
        raise

    else:
        step["ended_at"] = datetime.now().isoformat(timespec="seconds")
        step["duration_seconds"] = round(
            time.perf_counter() - timer, 5
        )
        step["status"] = "success"
        save_log()