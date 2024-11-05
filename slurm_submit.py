import argparse
import yaml
import os
import sys
import datetime
import subprocess
from subprocess import CalledProcessError

def build_sbatch_file(preamble: str, fillers: dict, dependency: str = None, command: str= None):
    """ Builds an sbatch file.

    Args:
        preamble: The preamble of the sbatch file. Should contain placeholders for the fillers.
        fillers: A dictionary of key-value pairs to fill in the placeholders in the preamble.
        dependency: The job ID of the job that this job depends on.
        command: The command to run in the sbatch file. Only needed if there is no dependency,
            otherwise the command will be read from the resume file of the dependency.
    """

    # Replace the placeholders in the preamble with the actual values from `fillers`
    # Placeholders are formatted as <key> in the preamble
    for key in fillers:
        preamble = preamble.replace(f"<{key}>", fillers[key])

    if dependency and command:
        raise ValueError("Cannot have both a dependency and directly specified command in the sbatch file.")
    
    if command:
        preamble += f"\n\n{command}"
    elif dependency: # use command read from resume file of the dependency
        preamble += f"\n" + "sleep 10" # Wait a bit to make sure the resume file is created
        preamble += f"\n" + "resume_command=`cat " + dependency + ".resume`"
        preamble += f"\n" + "eval $resume_command"
    else:
        raise ValueError("Must specify either a dependency or a command for the sbatch file.")

    return preamble
    

def launch_sbatch_file(sbatch_file_path: str, dependency: str = None):
    """ Submits an sbatch file to the Slurm queue.

    Args:
        sbatch_file_path: The path to the sbatch file to submit.
        dependency: The job ID of the job that this job depends.
    """

    try:
        commands = ['sbatch']
        if dependency:
            #commands += ["--dependency=afterany:" + dependency]
            commands += ["--dependency=aftercorr:" + dependency]
        commands += [sbatch_file_path]

        result = subprocess.run(commands, capture_output=True, text=True, check=True)
    
        # The job ID is included in the output like: "Submitted batch job 123456"
        output = result.stdout

        if "Submitted" in output:
            # Extract the job ID from the output:
            job_id = output.strip().split()[-1]
            print(f"Submitted job {job_id}" + (f" with dependency (chain-job) {dependency}" if dependency else ""))
            return job_id
        else:
            print("Error: Failed starting job. Could not find job ID in sbatch output.")
            return None

    except CalledProcessError as e:
        print(f"Error: Failed starting job. 'sbatch' returned non-zero exit code:")
        print(e.stdout)
        print(e.stderr)

        return None

if __name__ == "__main__":

    command_index = None
    for i in range(1, len(sys.argv)):
        if sys.argv[i].strip() == "cmd":
            # Everything after the 'cmd' is the command to run
            command_index = i

    if command_index is None:
        print("Error: No command specified (should be after 'cmd').")
        sys.exit(1)

    # Get the command to run
    full_command = " ".join(sys.argv[command_index + 1:])

    # Remove the command from the arguments
    sys.argv = sys.argv[:command_index]

    parser = argparse.ArgumentParser(
        description="Slurm submit helper."
    )

    parser.add_argument(
        "config",
        type=str,
        help="Name of the config file in the configs directory (without the .yaml extension)."
    )

    parser.add_argument(
        "-r", "--resumes",
        type=int,
        default=0,
        help="Number of times to resume the job (default 0)."
    )

    parser.add_argument(
        "-o", "--overwrites",
        type=str,
        help="Key value pairs to overwrite default values specified in the config file (format: key1=value1,key2=value2).",
    )

    parser.add_argument("-d", "--dry", action='store_true', help="Dry run, do not submit sbatch files.")

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
    scripts_dir = f"submit_scripts/{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    os.makedirs(scripts_dir, exist_ok=True)

    for i in range(0, args.resumes + 1):
        sbatch_file_path = f"{scripts_dir}/submit_{i}.sbatch"

        with open(sbatch_file_path, "w") as f:
            f.write(build_sbatch_file(preamble=config["preamble"], fillers=defaults, dependency=slurm_id if i > 0 else None, command=full_command if i == 0 else None))

        if not args.dry:
            slurm_id = launch_sbatch_file(sbatch_file_path, dependency=slurm_id if i > 0 else None)

            if slurm_id is None and i != args.resumes: # If failed and not the last resume job
                print("Stopping the submission of subsequent resume jobs due to an error.")
                break