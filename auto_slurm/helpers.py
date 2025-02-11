import time
import os


class RunTimer:
    def __init__(self, time_limit: int = 48):
        """Initialize the timer with a time limit in hours.

        Args:
            time_limit (int): The time limit in hours.
        """

        self.reset()
        self._time_limit = time_limit

    def reset(self):
        """Start the timer."""
        self._start_time = time.time()

    def time_limit_reached(self) -> bool:
        """Check if the time limit has been reached.

        Returns:
            bool: True if the time limit has been reached, False otherwise.
        """

        return (time.time() - self._start_time) > self._time_limit * 3600


def start_run(time_limit: int = 48):
    """Start the timer of a new run.

    Args:
        time_limit (int): The time limit in hours.

    Returns:
        RunTimer: The timer object.
    """

    run_timer = RunTimer(time_limit)
    return run_timer


def write_resume_file(command: str):
    """Write the command with which this process can be resumed to a file.

    Args:
        command (str): The command with which this process can be resumed.
    """

    slurm_id = os.environ.get("SLURM_JOB_ID", None)
    task_index = os.environ.get(
        "SLURM_SUBMIT_TASK_INDEX", 0
    )  # if this job has multiple tasks (processes) in one job

    if slurm_id is not None:
        with open(
            f"{slurm_id}" + (f"_{task_index}") + ".resume",
            "w",
        ) as f:
            f.write(command)

    else:
        raise RuntimeError(
            "SLURM_JOB_ID not set, probably not running in a slurm environment."
        )
