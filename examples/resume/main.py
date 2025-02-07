import argparse
import datetime
import os

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Example of a script that can be resumed for chained jobs."
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
    parser.add_argument(
        "--checkpoint_dir",
        type=str,
        default=None,
    )

    args = parser.parse_args()

    if args.iters_per_job is None:
        args.iters_per_job = args.max_iter

    if args.checkpoint_dir is not None:
        checkpoint_dir = args.checkpoint_dir
    else:
        # Generate checkpoint dir for this job using date and time as name
        checkpoint_dir = (
            f"checkpoints/{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        )
    os.makedirs(checkpoint_dir, exist_ok=True)

    if (
        args.start_iter + args.iters_per_job < args.max_iter
    ):  # If this job is not the last one!
        # Write to file how to resume this script in the next job
        slurm_id = os.environ.get("SLURM_JOB_ID", None)

        with open(f"{slurm_id}.resume", "w") as f:
            f.write(
                f"python main.py --start_iter {args.start_iter + args.iters_per_job} --max_iter {args.max_iter} --iters_per_job {args.iters_per_job} --checkpoint_dir {checkpoint_dir}"
            )

    ##### ... Do work ... #####

    print("Running job with the following parameters:")
    print(f"start_iter: {args.start_iter}")
    print(f"max_iter: {args.max_iter}")
    print(f"iters_per_job: {args.iters_per_job}")
    print(f"checkpoint_dir: {checkpoint_dir}")