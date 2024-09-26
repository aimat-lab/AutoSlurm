# Slurm submission helper

This submission helper allows you to automatically generate slurm submission scripts and start them for you.
Supports simple jobs, splitting multiple scripts accross the GPUs of a node, and even chain jobs - all
with a single command.

At the moment, only single-node jobs that use all the GPUs of that node are supported.
Chain jobs of that type of job are also supported, which at the moment is the main use of this tool.

## Dependencies
The only dependency is `pyyaml`, so likely most existing environments can be used.

## Setup

The easiest way to be able to run the command `slurm_submit` from anywhere is to add the following lines to your `~/.bashrc` file:

```
function slurm_submit() {
    python /path/to/repository/slurm_submit.py "$@"
}
```

## Starting a simple non-chained job

```
usage: slurm_submit.py [-h] [-r RESUMES] [-o OVERWRITES] [-d] config command

positional arguments:
  config                Name of the config file in the configs directory (without the .yaml extension).
  command               Command to run in the sbatch file. Should be put in quotes.

options:
  -h, --help            show this help message and exit

  -r RESUMES, --resumes RESUMES
                        Number of times to resume the job (default 0).

  -o OVERWRITES, --overwrites OVERWRITES
                        Key value pairs to overwrite default values specified in the config file (format: key1=value1,key2=value2).

  -d, --dry             Dry run, do not submit sbatch files.
```