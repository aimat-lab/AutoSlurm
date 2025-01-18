import argparse
import yaml
import os
import sys
import datetime
import subprocess
from subprocess import CalledProcessError
from typing import List


def build_sbatch_file(
    preamble: str,
    fillers: dict,
    dependency: str = None,
    commands: List[str] = None,
    dont_assign_gpu: bool = False,
    run_tasks_in_parallel: bool = False,
):
    """Builds an sbatch file.

    Args:
        preamble: The preamble of the sbatch file. Should contain placeholders for the fillers.
        fillers: A dictionary of key-value pairs to fill in the placeholders in the preamble.
        dependency: The job ID of the job that this job depends on.
        commands: The commands to run in the sbatch file. Only needed if there is no dependency,
            otherwise the commands will be read from the resume files of the dependency.
        dont_assign_gpu: When using the parallel mode, by default we assign GPUs with increasing index to each task in the job. This flag disables this behavior.
        run_tasks_in_parallel: Run tasks of this job in parallel instead of sequentially.
    """

    if run_tasks_in_parallel:
        if "<additional_sbatch_configs>" in preamble:
            fillers["additional_sbatch_configs"] = (
                "#SBATCH --output=/dev/null" + "\n" + "#SBATCH --error=/dev/null"
            )
        else:
            print(
                "Warning: <additional_sbatch_configs> not found in the preamble."
                + "\nWhen using the parallel mode, it is recommended to redirect"
                + "\nthe output to /dev/null, such that the output of the individual"
                + "\ntasks can be written to separate files."
            )

    # Replace the placeholders in the preamble with the actual values from `fillers`
    # Placeholders are formatted as <key> in the preamble
    for key in fillers:
        preamble = preamble.replace(f"<{key}>", fillers[key])

    if dependency and len([c for c in commands if c is not None]) > 0:
        raise ValueError(
            "Cannot have both a dependency and directly specified commands."
        )

    if dependency:
        preamble += (
            f"\n" + "sleep 10"
        )  # Wait a bit to make sure the resume files are written

    for i, command in enumerate(commands):
        if command is not None:
            command_line = command
        elif dependency:  # Use command read from resume file of the dependency
            preamble += (
                f"\n" + "resume_command=`cat " + dependency + "_" + str(i) + ".resume`"
            )
            command_line = "eval $resume_command"

        else:
            raise ValueError("Must specify either a dependency or a command.")

        if run_tasks_in_parallel:
            command_line += (
                " &> slurm-${SLURM_JOB_ID}"
                + (f"_{i}" if len(commands) > 1 else "")
                + ".out &"
            )

            if not dont_assign_gpu:
                command_line = f"CUDA_VISIBLE_DEVICES={i} " + command_line

        # Let the command know which task index it is, such that it can write to the correct resume file:
        command_line = "SLURM_SUBMIT_TASK_INDEX=" + str(i) + " " + command_line

        preamble += f"\n" + command_line

    if run_tasks_in_parallel:
        preamble += f"\n\nwait"  # Wait for all tasks to finish

    return preamble


def launch_sbatch_file(sbatch_file_path: str, dependency: str = None):
    """Submits an sbatch file to the Slurm queue.

    Args:
        sbatch_file_path: The path to the sbatch file to submit.
        dependency: The job ID of the job that this job depends.
    """

    try:
        commands = ["sbatch"]
        if dependency:
            # commands += ["--dependency=afterany:" + dependency]
            commands += ["--dependency=aftercorr:" + dependency]
        commands += [sbatch_file_path]

        result = subprocess.run(commands, capture_output=True, text=True, check=True)

        # The job ID is included in the output like: "Submitted batch job 123456"
        output = result.stdout

        if "Submitted" in output:
            # Extract the job ID from the output:
            job_id = output.strip().split()[-1]
            print(
                f"Submitted job {job_id}"
                + (f" with dependency (chain-job) {dependency}" if dependency else "")
            )
            return job_id
        else:
            print("Error: Failed starting job. Could not find job ID in sbatch output.")
            return None

    except CalledProcessError as e:
        print(f"Error: Failed starting job. 'sbatch' returned non-zero exit code:")
        print(e.stdout)
        print(e.stderr)

        return None


def main():
    if not len(sys.argv) == 1 and not (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
        command_start_indices = []
        command_repetitions = []
        for i in range(1, len(sys.argv)):
            if sys.argv[i].strip().startswith("cmd"):
                command_start_indices.append(i)

                command_repetition = (
                    int(sys.argv[i].strip().split("x")[1]) if "x" in sys.argv[i] else 1
                )
                command_repetitions.append(command_repetition)

        if len(command_start_indices) == 0:
            print("Error: No command specified (should be after 'cmd').")
            sys.exit(1)

        # Get list of commands:
        commands = []
        for i in range(len(command_start_indices)):
            command_index = command_start_indices[i]
            if i == len(command_start_indices) - 1:
                command = " ".join(sys.argv[command_index + 1 :])
            else:
                command = " ".join(
                    sys.argv[command_index + 1 : command_start_indices[i + 1]]
                )

            commands.append([command] * command_repetitions[i])

        # Remove the commands from sys.argv
        sys.argv = sys.argv[: command_start_indices[0]]

    parser = argparse.ArgumentParser(description="Slurm submit helper.")

    parser.add_argument(
        "config",
        type=str,
        help="Name of the config file in the configs directory (without the .yaml extension).",
    )

    parser.add_argument(
        "-r",
        "--resumes",
        type=int,
        default=0,
        help="Number of times to resume the job (default 0).",
    )

    parser.add_argument(
        "-o",
        "--overwrites",
        type=str,
        help="Key value pairs to overwrite default values specified in the config file (format: key1=value1,key2=value2).",
    )

    parser.add_argument(
        "-d",
        "--dry",
        action="store_true",
        help="Dry run, do not submit sbatch files, just create them.",
    )

    parser.add_argument(
        "-p",
        "--parallel",
        action="store_true",
        help="Run tasks of this job in parallel.",
    )

    parser.add_argument(
        "-dagpu",
        "--dont_assign_gpu",
        action="store_true",
        help="When using the parallel mode (-p), by default we assign GPUs with increasing index to each task in the job. This flag disables this behavior.",
    )

    args = parser.parse_args()

    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_file_path = os.path.join(dir_path, "configs", args.config + ".yaml")
    config_file = open(config_file_path, "r")
    config = yaml.safe_load(config_file)
    config_file.close()

    defaults = config["defaults"]
    if args.overwrites:
        for overwrite in args.overwrites.split(","):
            key, value = overwrite.split("=")
            defaults[key] = value

    # Create directory for sbatch files
    scripts_dir = (
        f"submit_scripts/{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    )
    os.makedirs(scripts_dir, exist_ok=True)
    print("Slurm sbatch files will be written to:", scripts_dir)

    for i in range(0, args.resumes + 1):
        sbatch_file_path = f"{scripts_dir}/submit_{i}.sbatch"

        with open(sbatch_file_path, "w") as f:
            f.write(
                build_sbatch_file(
                    preamble=config["preamble"],
                    fillers=defaults,
                    dependency=slurm_id if i > 0 else None,
                    commands=(
                        commands if i == 0 else [None] * len(commands)
                    ),  # Signal that command(s) should be taken from the resume file(s)
                    dont_assign_gpu=args.dont_assign_gpu,
                    run_tasks_in_parallel=args.parallel,
                )
            )

        if not args.dry:
            slurm_id = launch_sbatch_file(
                sbatch_file_path, dependency=slurm_id if i > 0 else None
            )

            if (
                slurm_id is None and i != args.resumes
            ):  # If failed and not the last resume job
                print(
                    "Stopping the submission of subsequent resume jobs due to an error."
                )
                break
        else:
            slurm_id = str(i)
            print(
                "Warning: Using dry run with resume jobs. Slurm ID is set to", slurm_id
            )
