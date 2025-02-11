import argparse
import time
from auto_slurm.helpers import start_run
from auto_slurm.helpers import write_resume_file

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Example script that can be resumed.")
    parser.add_argument(
        "--checkpoint_path",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--start_iter",
        type=int,
        default=0,
    )
    args = parser.parse_args()

    if args.checkpoint_path is not None:

        # ... Checkpoint loading goes here ...

        pass

    start_iter = args.start_iter

    timer = start_run(
        time_limit=10 / 3600
    )  # 10s timelimit here, in practice of course longer

    # Training loop:
    max_iter = 100
    for i in range(start_iter, max_iter):
        print(f"Training iteration {i}")
        time.sleep(1)  # Do work ...

        if timer.time_limit_reached() and i < max_iter - 1:
            # Time limit reached and still work to do
            # => Write checkpoint + resume file to pick up the work:

            # ... Checkpoint saving goes here ...

            write_resume_file(
                "python main.py --checkpoint_path my_checkpoint.pt --start_iter "
                + str(i + 1)
            )

            # After writing this resume file, auto_slurm will automatically schedule a new job that resumes the work using the command in the resume file.

            break
