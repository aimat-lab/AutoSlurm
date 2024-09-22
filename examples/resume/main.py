import argparse
import datetime
import os

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Resume example"
    )

    parser.add_argument(
        "--start_iter",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--max_iter",
        type=int,
        default=1000,
    )
    parser.add_argument(
        "--iters_per_job",
        type=int,
        default=None,
    )

    args = parser.parse_args()

    if args.iters_per_job is None:
        args.iters_per_job = args.max_iter

    # Generate checkpoint dir for this job using date and time as name
    checkpoint_dir = f"checkpoints/{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    os.makedirs(checkpoint_dir, exist_ok=True)

    if args.start_iter + args.iters_per_job < args.max_iter:
        # Write to file how to resume this script in the next job
        # Get the current slurm id from the environment variable
        slurm_id = os.environ.get("SLURM_JOB_ID", None)

        with open(f"{slurm_id}.resume", "w") as f:
            f.write(f"--start_iter {args.start_iter + args.iters_per_job} --max_iter {args.max_iter} --iters_per_job {args.iters_per_job}")