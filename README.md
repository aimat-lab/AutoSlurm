# Slurm submission helper

This submission helper allows you to automatically generate slurm submission scripts and starts them for you, based
on reusable templates. Next to normal jobs, also the automatic creation of chain jobs is supported.

While some basic templates for sbatch scripts are available (see `configs` directory), you probably want to modify them
slightly or create your own templates.

## Setup

Simply clone the repository and install as a package:

```
pip install -e .
```

Then, the command "slurm_submit" will be available to start jobs.


## Starting a simple non-chained job

```
usage: slurm_submit [-h] [-r RESUMES] [-o OVERWRITES] [-d] config cmd [Your command]

Slurm submit helper.

positional arguments:
  config                Name of the config file in the configs directory (without the .yaml extension).

options:
  -h, --help            show this help message and exit
  -r RESUMES, --resumes RESUMES
                        Number of times to resume the job (default 0).
  -o OVERWRITES, --overwrites OVERWRITES
                        Key value pairs to overwrite default values specified in the config file (format: key1=value1,key2=value2).
  -d, --dry             Dry run, do not submit sbatch files.
```