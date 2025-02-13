import argparse
import os
import sys
import datetime
import subprocess
import re
import math
import uuid
import shutil
from subprocess import CalledProcessError
from typing import List
import hydra
import omegaconf
from typing import Optional
from itertools import product
from auto_slurm.config import Config
from auto_slurm.config import GeneralConfig


def parse_int_or_none(value: str):
    if value.lower() == "none":
        return None
    try:
        return int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid value {value}. Must be an integer or 'None'."
        )


def build_commands_str(
    commands: List[str], job_start_task_index: int, gpus_per_task: Optional[int] = None
) -> str:
    """
    Builds a string of multiple commands to run in a slurm job script.

    Args:
        commands: The list of commands to run.
        job_start_task_index: The task index of the first task.
        gpus_per_task: The number of GPUs to assign to each task. If None, the tasks will not be assigned GPUs.

    Returns:
        A string of commands to run in a slurm job script.
    """

    commands_str = ""

    gpu_counter = 0
    for i, command in enumerate(commands):
        command_line = (
            command
            + " &> slurm-${SLURM_JOB_ID}"
            + (f"_{job_start_task_index+i}" if len(commands) > 1 else "")
            + ".out &"
        )

        if gpus_per_task is not None:
            # Assign the correct GPU(s) to the task:
            command_line = (
                f"CUDA_VISIBLE_DEVICES={','.join([str(j) for j in range(gpu_counter, gpu_counter + gpus_per_task)])} "
                + command_line
            )

        # Let the command know which task index it is, such that it can write to the correct resume file:
        command_line = (
            "SLURM_SUBMIT_TASK_INDEX="
            + str(job_start_task_index + i)
            + " "
            + command_line
        )

        commands_str += "\n" + command_line

        if gpus_per_task is not None:
            gpu_counter += gpus_per_task

    return commands_str


def create_slurm_job_files(
    main_script_path: str,
    resume_script_path: str,
    job_start_task_index: int,
    template: str,
    fillers: dict,
    global_fillers: dict,
    commands: List[str] = None,
    gpus_per_task: Optional[int] = None,
):
    """Builds the main and resume slurm job files and writes them to disk.

    Args:
        main_script_path: The path to the main slurm job file to write.
        resume_script_path: The path to the resume slurm job file to write.
        job_start_index: The task index of the first task.
        template: The template of the slurm job file. Should contain placeholders for the fillers.
        fillers: A dictionary of key-value pairs to fill in the placeholders in the template.
        global_fillers: A dictionary of key-value pairs to fill in the placeholders in the template.
            Applied after `fillers`, such that placeholders inside the values of `fillers` are replaced.
        commands: The commands to run in the slurm job.
        gpus_per_task: The number of GPUs to assign to each task. If None, the tasks will not be assigned GPUs.
    """

    for key in fillers:
        template = template.replace(f"<{key}>", fillers[key])
    for key in global_fillers:
        template = template.replace(f"<{key}>", global_fillers[key])

    main_slurm_script = template
    resume_slurm_script = template

    ##### Build the main slurm script #####

    main_slurm_script += (
        f"{build_commands_str(commands, job_start_task_index, gpus_per_task)}"
    )
    main_slurm_script += f"\n\nwait"  # Wait for all tasks to finish
    main_slurm_script += "\nsleep 10"  # Just to be safe

    ##### Build the resume slurm script #####

    resume_commands = [
        f"eval `cat ./.aslurm/${{PREVIOUS_SLURM_ID}}_{job_start_task_index+i}.resume`"
        for i in range(len(commands))
    ]

    resume_slurm_script += (
        f"{build_commands_str(resume_commands, job_start_task_index, gpus_per_task)}"
    )
    resume_slurm_script += f"\n\nwait"  # Wait for all tasks to finish
    resume_slurm_script += "\nsleep 10"  # Just to be safe

    ##### Run resume job if needed #####

    # In both the main and resume script, we check if any resume files have been created (SLURM_JOB_ID_*.resume).
    # If yes, we schedule a new job to resume the tasks.

    resume_str = (
        '\n\nif compgen -G "./.aslurm/${SLURM_JOB_ID}_*.resume" > /dev/null; then'
    )
    resume_str += "\n\texport PREVIOUS_SLURM_ID=${SLURM_JOB_ID}"
    resume_str += f"\n\tsbatch {resume_script_path}"
    resume_str += "\nfi"

    main_slurm_script += resume_str
    resume_slurm_script += resume_str

    ##### Write the scripts to file #####

    with open(main_script_path, "w") as f:
        f.write(main_slurm_script)
    with open(resume_script_path, "w") as f:
        f.write(resume_slurm_script)


def launch_slurm_job(slurm_file_path: str, job_index: int):
    """Submits a slurm job to the slurm queue.

    Args:
        slurm_file_path: The path to the slurm job file to submit.
        job_index: The index of the job.
    """

    try:
        commands = ["sbatch"]
        commands += [slurm_file_path]

        result = subprocess.run(commands, capture_output=True, text=True, check=True)

        # The job ID is included in the output as such: "Submitted batch job 123456"
        output = result.stdout
        if "Submitted" in output:
            # Extract the job ID from the output:
            job_id = output.strip().split()[-1]
            print(f"Submitted job {job_index} with slurm_id {job_id}.")
        else:
            print(
                f"Error: Failed starting job {job_index}. Could not find job ID in sbatch output."
            )

    except CalledProcessError as e:
        print(
            f"Error: Failed starting job {job_index}. 'sbatch' returned non-zero exit code:"
        )
        print(e.stdout)
        print(e.stderr)


def expand_commands(commands: List[str]) -> List[str]:
    expanded_commands = []

    for command in commands:
        # Find all instances of <[]> and <{}> syntax:
        bracket_matches = re.findall(r"<\[(.*?)\]>", command)
        brace_matches = re.findall(r"<\{(.*?)\}>", command)

        if bracket_matches and brace_matches:
            raise ValueError("Cannot mix <[]> and <{}> syntax in the same command.")

        if bracket_matches:
            options = [
                [subitem.strip() for subitem in item.split(",")]
                for item in bracket_matches
            ]
            if any(len(opt) != len(options[0]) for opt in options):
                raise ValueError("Paired lists must have the same length.")

            for values in zip(*options):
                temp_command = command
                for match, value in zip(bracket_matches, values):
                    temp_command = temp_command.replace(f"<[{match}]>", value, 1)
                expanded_commands.append(temp_command)

        elif brace_matches:
            options = [
                [subitem.strip() for subitem in item.split(",")]
                for item in brace_matches
            ]

            for values in product(*options):
                temp_command = command
                for match, value in zip(brace_matches, values):
                    temp_command = temp_command.replace(f"<{{{match}}}>", value, 1)
                expanded_commands.append(temp_command)

        else:
            expanded_commands.append(command)

    return expanded_commands


def main():
    if len(sys.argv) == 1 or (not (sys.argv[1] == "-h" or sys.argv[1] == "--help")):
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
            print(
                "Error: No command specified (should be after 'cmd' in the end of the bash command)."
            )
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

            commands.extend([command] * command_repetitions[i])

        commands = expand_commands(
            commands
        )  # Expand using the <[ ... ]> and <{ ... }> shorthand syntax for sweeps

        # Remove the commands from sys.argv
        sys.argv = sys.argv[: command_start_indices[0]]

    parser = argparse.ArgumentParser(
        description="Slurm submission helper that writes slurm job scripts based on templates and starts them for you."
    )

    parser.add_argument(
        "-cn",
        "--config",
        type=str,
        help="Name of the template config file in the configs directory (without the .yaml extension).",
        required=False,
        default=None,
    )

    parser.add_argument(
        "-o",
        "--overwrite_fillers",
        type=str,
        help="Key value pairs to overwrite default filler values specified in the template config file (format: key1=value1,key2=value2).",
        required=False,
        default=None,
    )

    parser.add_argument(
        "-d",
        "--dry",
        action="store_true",
        help="Dry run, do not submit slurm jobs, just create the job files.",
    )

    parser.add_argument(
        "-gpt",
        "--gpus_per_task",
        type=str,
        help="Number of GPUs to assign to each task. This overwrites the template config file setting.",
        required=False,
        default="",
    )

    parser.add_argument(
        "-gpus",
        "--NO_gpus",
        type=str,
        help="Total number of GPUs per job. This overwrites the template config file setting.",
        required=False,
        default="",
    )

    parser.add_argument(
        "-mt",
        "--max_tasks",
        type=str,
        help="Maximum number of tasks per job. This overwrites the template config file setting.",
        required=False,
        default="",
    )

    if not os.path.exists(
        os.path.expanduser("~/.config/auto_slurm/general_config.yaml")
    ):
        os.makedirs(os.path.expanduser("~/.config/auto_slurm"), exist_ok=True)
        shutil.copy(
            os.path.join(os.path.dirname(__file__), "general_config.yaml"),
            os.path.expanduser("~/.config/auto_slurm/general_config.yaml"),
        )
        print("Created default general_config.yaml in ~/.config/auto_slurm\n")

    args = parser.parse_args()

    general_config_path = os.path.relpath(
        os.path.expanduser("~/.config/auto_slurm"),
        start=os.path.dirname(__file__),
    )  # Needs to be relative to the current script!
    with hydra.initialize(config_path=general_config_path, version_base=None):
        cfg = hydra.compose(config_name="general_config")
        cfg_dict = omegaconf.OmegaConf.to_container(
            cfg, resolve=True, throw_on_missing=True
        )
        general_config: GeneralConfig = GeneralConfig(**cfg_dict)

    if args.config is None:  # Get the config using the configured hostname mappings
        default_hostname_config_mappings = general_config.hostname_config_mappings
        hostname = subprocess.run(
            ["hostname"], capture_output=True, text=True, check=True
        ).stdout.strip()

        # Check if any of the mappings match the hostname (regex):
        count_matches = 0
        for current_hostname, config in default_hostname_config_mappings.items():
            # Check if the hostname matches the regex:
            if re.match(current_hostname, hostname) is not None:
                args.config = config
                count_matches += 1

        if count_matches == 0:
            raise RuntimeError(
                f"No config specified and no default config found for the current hostname '{hostname}'."
            )
        elif count_matches > 1:
            raise RuntimeError(
                f"Multiple default configs found that match the current hostname '{hostname}'."
            )

        print(f"Matched hostname '{hostname}' to config '{args.config}'.")

    with hydra.initialize("./configs/", version_base=None):
        cfg = hydra.compose(config_name=args.config)
        cfg_dict = omegaconf.OmegaConf.to_container(
            cfg, resolve=True, throw_on_missing=True
        )
        config: Config = Config(**cfg_dict)

        if args.gpus_per_task != "":
            config.gpus_per_task = parse_int_or_none(args.gpus_per_task)
        if args.NO_gpus != "":
            config.NO_gpus = parse_int_or_none(args.NO_gpus)
        if args.max_tasks != "":
            config.max_tasks = parse_int_or_none(args.max_tasks)

    default_fillers = config.default_fillers
    if args.overwrite_fillers is not None:
        for overwrite in args.overwrite_fillers.split(","):
            key, value = overwrite.split("=")
            default_fillers[key] = value

    # Create directory for slurm job files
    scripts_dir = f".aslurm/{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{str(uuid.uuid4())[0:7]}"
    os.makedirs(scripts_dir, exist_ok=True)
    print("Slurm job files will be written to", scripts_dir)

    # Determine how many jobs we need to run:
    num_tasks = len(commands)
    if config.NO_gpus is not None:
        tasks_per_job = int(config.NO_gpus / config.gpus_per_task)
    else:
        tasks_per_job = config.max_tasks
    NO_jobs = math.ceil(num_tasks / tasks_per_job)

    print(
        f"Splitting the {len(commands)} tasks into {NO_jobs} job(s) (max. {tasks_per_job} task(s) per job"
        + (
            f" using {config.gpus_per_task} GPU(s) per task"
            if config.gpus_per_task is not None
            else ""
        )
        + ")."
    )

    task_index_counter = 0
    for i in range(NO_jobs):
        main_script_path = os.path.join(scripts_dir, f"main_{i}.sh")
        resume_script_path = os.path.join(scripts_dir, f"resume_{i}.sh")

        create_slurm_job_files(
            main_script_path=main_script_path,
            resume_script_path=resume_script_path,
            job_start_task_index=task_index_counter,
            template=config.template,
            fillers=default_fillers,
            global_fillers=general_config.global_fillers,
            commands=commands[task_index_counter : task_index_counter + tasks_per_job],
            gpus_per_task=config.gpus_per_task,
        )

        if not args.dry:
            launch_slurm_job(main_script_path, job_index=i)

        task_index_counter += tasks_per_job


if __name__ == "__main__":
    main()
