# AutoSlurm

This tool automatically generates slurm submission scripts based on reusable templates and starts them for you.
This includes support for multi-task multi-GPU jobs, automatic creation of (infinite) chain jobs, and easy support for hyperparameter sweeps.

While some basic templates for sbatch scripts are available (see `configs` directory), you probably want to modify them
slightly or create your own templates.

## Setup

Simply clone the repository and install as a package:

```
pip install -e .
```

Then, the command `aslurm` will be available to start jobs.


## Single jobs

```
usage: aslurm [-h] [-r RESUMES] [-o OVERWRITES] [-d] config cmd [Your command]

Slurm submit helper.

positional arguments:
  config                Name of the config file in the configs directory (without the .yaml extension).

options:
  -h, --help            show help message and exit
  -r RESUMES, --resumes RESUMES
                        Number of times to resume the job (default 0).
  -o OVERWRITES, --overwrites OVERWRITES
                        Key value pairs to overwrite default values specified in the config file (format: key1=value1,key2=value2).
  -d, --dry             Dry run, do not submit sbatch files, just create them.
```

Make sure to not forget the `cmd` keyword when calling the script. Everything after `cmd` will be used as 
the command to be executed in the sbatch script. Furthermore, make sure to properly escape quote characters when necessary,
otherwise they will be eaten by bash.

Example usage:

```
aslurm -o env=my_env time=5-00:00:00 haicore_full cmd python train.py -cn myconfig.yaml \"wandb_notes=\'My cool note to remember what the hell I was running.\'\"
```

## Multi jobs

## Chain jobs

### Traditional chain jobs