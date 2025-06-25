import time
import os
import pathlib

import jinja2 as j2

PATH: str = pathlib.Path(__file__).parent.resolve()

TEMPLATE_PATH: str = os.path.join(PATH, "templates")

TEMPLATE_ENV = j2.Environment(
    loader=j2.FileSystemLoader(TEMPLATE_PATH),
    autoescape=j2.select_autoescape(),
)

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
        os.makedirs("./.aslurm", exist_ok=True)
        with open(
            f"./.aslurm/{slurm_id}" + (f"_{task_index}") + ".resume",
            "w",
        ) as f:
            f.write(command)

    else:
        raise RuntimeError(
            "SLURM_JOB_ID not set, probably not running in a slurm environment."
        )


def get_version() -> str:
    """
    Returns the current version of the package as it is written in the ``VERSION`` file.
    
    Returns:
        str: The version of the package.
    """
    version_path: str = os.path.join(PATH, "VERSION")
    with open(version_path, "r") as file:
        version: str = file.read().strip()
        
    return version


def create_slurm_jobs(
    fillers: dict,
    commands: list[str],
    options: dict[str, any], 
    main_template: j2.Template = TEMPLATE_ENV.get_template('main.sh.j2'),
    resume_template: j2.Template = TEMPLATE_ENV.get_template('resume.sh.j2'),
) -> tuple[str, str]:
    """
    Generate SLURM job scripts for a batch of commands using Jinja2 templates.

    This function creates the main and resume SLURM job scripts by rendering the provided Jinja2 templates
    with the given commands, options, and additional context.

    Args:
        job_start_task_index (int):
            The starting index for the SLURM array job tasks. Used for job/task indexing in templates.
        fillers (dict):
            Additional variables to be passed to the templates for rendering.
        commands (list[str]):
            A list of shell commands to be executed by the SLURM job array. Each command typically represents a single task.
        options (dict[str, any]):
            Dictionary of SLURM/job options, including resource requirements and custom settings. May include 'gpus_per_task'.
        main_template (j2.Template, optional):
            Jinja2 template for the main SLURM job script. Defaults to the 'main.sh.j2' template.
        resume_template (j2.Template, optional):
            Jinja2 template for the resume SLURM job script. Defaults to the 'resume.sh.j2' template.

    Returns:
        tuple[str, str]:
            A tuple containing:
                - The rendered main SLURM job script as a string.
                - The rendered resume SLURM job script as a string.

    Notes:
        - The CUDA_VISIBLE_DEVICES assignments are generated based on the number of commands and GPUs per task.
        - The templates are expected to use the variables: fillers, commands, options, gpus, and gpus_per_task.
        - This function does not write any files; it only returns the rendered script contents.
    """

    # This list will be the same length as the commands list and contain the strings which will be used 
    # for the CUDA_VISIBLE_DEVICES environment variable in the slurm job script based on the number
    # of gpus_per_task configured in the options.
    gpus: list[str] = []
    gpus_per_task = options.get("gpus_per_task", None)
    if gpus_per_task is not None and gpus_per_task > 0:
        for i in range(0, len(commands) * gpus_per_task, gpus_per_task):
            gpus.append(",".join(str(j) for j in range(i, i + gpus_per_task)))

    main_script_content: str = main_template.render(
        fillers=fillers,
        commands=commands,
        options=options,
        gpus=gpus,
    )
    
    resume_script_content: str = resume_template.render(
        fillers=fillers,
        commands=commands,
        options=options,
        gpus_per_task=gpus_per_task,
        gpus=gpus
    )
    
    return main_script_content, resume_script_content
