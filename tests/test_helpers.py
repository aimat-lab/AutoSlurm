import os
import pytest
import jinja2 as j2

from .utils import ASSETS_PATH, ARTIFACTS_PATH
from auto_slurm.helpers import TEMPLATE_ENV
from auto_slurm.helpers import create_slurm_jobs


def test_saving_artifacts():
    file_path = os.path.join(ARTIFACTS_PATH, "test_artifact.txt")
    with open(file_path, "w") as f:
        f.write("This is a test artifact.")
    
    assert os.path.exists(file_path), "Artifact file was not created."


def test_create_slurm_jobs_basic():
    # Use actual templates from the templates directory with absolute path loader
    main_template = TEMPLATE_ENV.get_template("main.sh.j2")
    resume_template = TEMPLATE_ENV.get_template("resume.sh.j2")

    fillers = {"user": "test"}
    commands = ["echo 1", "echo 2"]
    options = {"gpus_per_task": 2}

    main_script, resume_script = create_slurm_jobs(
        fillers=fillers,
        commands=commands,
        options=options,
        main_template=main_template,
        resume_template=resume_template,
    )

    # Save artifacts
    with open(os.path.join(ARTIFACTS_PATH, "main_basic.sh"), "w") as f:
        f.write(main_script)
    with open(os.path.join(ARTIFACTS_PATH, "resume_basic.sh"), "w") as f:
        f.write(resume_script)

    # Check that commands are in the output
    assert "echo 1" in main_script and "echo 2" in main_script
    assert "echo 1" in resume_script and "echo 2" in resume_script


def test_create_slurm_jobs_no_gpus():
    main_template = TEMPLATE_ENV.get_template("main.sh.j2")
    resume_template = TEMPLATE_ENV.get_template("resume.sh.j2")

    fillers = {"user": "test"}
    commands = ["run something"]
    options = {}  # No GPUs specified

    main_script, resume_script = create_slurm_jobs(
        fillers=fillers,
        commands=commands,
        options=options,
        main_template=main_template,
        resume_template=resume_template,
    )

    # Save artifacts
    with open(os.path.join(ARTIFACTS_PATH, "main_no_gpus.sh"), "w") as f:
        f.write(main_script)
    with open(os.path.join(ARTIFACTS_PATH, "resume_no_gpus.sh"), "w") as f:
        f.write(resume_script)

    assert "run something" in main_script
    assert "run something" in resume_script


def test_create_slurm_jobs_empty_commands():
    main_template = TEMPLATE_ENV.get_template("main.sh.j2")
    resume_template = TEMPLATE_ENV.get_template("resume.sh.j2")

    fillers = {"user": "test"}
    commands = []
    options = {"gpus_per_task": 1}

    main_script, resume_script = create_slurm_jobs(
        fillers=fillers,
        commands=commands,
        options=options,
        main_template=main_template,
        resume_template=resume_template,
    )

    # Save artifacts
    with open(os.path.join(ARTIFACTS_PATH, "main_empty.sh"), "w") as f:
        f.write(main_script)
    with open(os.path.join(ARTIFACTS_PATH, "resume_empty.sh"), "w") as f:
        f.write(resume_script)

    # Check that the script is not empty and has a SLURM shebang
    assert main_script.strip() != ""
    assert main_script.strip().startswith("#!/bin/bash")
    assert resume_script.strip() != ""
    assert resume_script.strip().startswith("#!/bin/bash")