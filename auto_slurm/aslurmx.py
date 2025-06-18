import os
import sys
import pathlib
import datetime
import subprocess
import uuid
import rich_click as click

import rich
import yaml
import hydra
import omegaconf
from rich.pretty import pprint
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.style import Style
from rich.syntax import Syntax
from rich.padding import Padding
from auto_slurm.helpers import PATH, TEMPLATE_PATH, TEMPLATE_ENV
from auto_slurm.helpers import get_version
from auto_slurm.helpers import create_slurm_jobs
from auto_slurm.config import AutoSlurmConfig
from auto_slurm.config import GeneralConfig, Config
from jinja2 import FileSystemLoader, ChoiceLoader


class RichLogo:
    
    STYLE = Style(bold=True, color='white')
    
    def __rich_console__(self, console, options):
        logo_path = os.path.join(TEMPLATE_PATH, 'logo.txt')
        with open(logo_path, mode='r') as file:
            logo_string: str = file.read()
            text = Text(logo_string, style=self.STYLE)
            pad = Padding(text, (1, 1))
            yield pad


class RichHelp:
    
    def __rich_console__(self, console, options):
        yield "AutoSlurm Command Line Interface.\n"
        yield Text((
            "This is a tool to simplify the scheduling of SLURM jobs across different HPC environments. "
            "You can schedule a new slurm job using the 'cmd' command like this:"
        ))
        yield Padding(Syntax((
            "aslurm -cn <config_name> cmd python script.py --arg1=100"
        ), lexer='bash', theme='monokai', line_numbers=False), (1, 5))

class RichConfigList:
    
    def __init__(self, config_map: dict[str, dict]):
        self.config_map = config_map
        
        self.column_names: list[str] = [
            'Config Name',
            'Time',
            'Memory',
            'CPUs',
            'GRES',
        ]
        
        self.rows: list[list] = []
        for config_name, config_content in self.config_map.items():
            if 'default_fillers' in config_content:
                row: list = [
                    config_name,
                    config_content['default_fillers'].get('time', 'N/A'),
                    config_content['default_fillers'].get('mem', 'N/A'),
                    config_content['default_fillers'].get('cpus', 'N/A'),
                    config_content['default_fillers'].get('gres', 'N/A'),
                ]
                self.rows.append(row)
        
    def __rich_console__(self, console, options):
        
        table = Table(
            show_header=True,
            header_style="bold magenta",
            expand=True,
            title="Available AutoSlurm Configs"
        )
        # Make the first column expand to fill available space
        table.add_column(self.column_names[0], style="bold", no_wrap=False, ratio=2)
        for col in self.column_names[1:]:
            table.add_column(col)

        for row in self.rows:
            # Make the first column bold
            table.add_row(f"[bold]{row[0]}[/bold]", *[str(cell) for cell in row[1:]])

        yield table


class KeyValueList(click.ParamType):
    """
    A custom Click parameter type for parsing comma-separated key-value pairs from the command line.

    This class allows users to specify multiple key-value pairs as a single command-line argument,
    using the format: key1=value1,key2=value2,... The resulting value is converted into a Python
    dictionary mapping each key to its corresponding value.

    Example usage in a Click option:
        @click.option('--overwrite-fillers', '-o', type=KeyValueList(), default={}, help='Overwrite fillers for the config.')

    Example command-line input:
        --overwrite-fillers time=01:00:00,mem=16G,cpus=4

    This would result in:
        {'time': '01:00:00', 'mem': '16G', 'cpus': '4'}

    Notes:
        - Whitespace around keys and values is stripped.
        - If the input is empty or None, an empty dictionary is returned.
        - Invalid pairs (missing '=') or empty keys will raise a Click error.
    """

    name = "keyvaluelist"

    def convert(self, value, param, ctx):
        """
        Converts a comma-separated string of key-value pairs into a dictionary.

        Args:
            value (str): The input string from the command line, e.g., "key1=val1,key2=val2".
            param: The Click parameter object (unused).
            ctx: The Click context object (unused).

        Returns:
            dict: A dictionary mapping keys to values as parsed from the input string.

        Raises:
            click.BadParameter: If a pair does not contain '=', or if a key is empty.

        Example:
            >>> KeyValueList().convert("foo=bar,baz=qux", None, None)
            {'foo': 'bar', 'baz': 'qux'}
        """
        # If the input is empty or None, return an empty dictionary
        if not value:
            return {}

        result = {}
        # Split the input string by commas to get individual key-value pairs
        pairs = value.split(",")
        for pair in pairs:
            # Each pair must contain an '=' character
            if "=" not in pair:
                self.fail(
                    f"Invalid key-value pair: '{pair}'. Expected format: key=value",
                    param,
                    ctx
                )
            # Split only on the first '=' to allow '=' in values
            key, val = pair.split("=", 1)
            key = key.strip()
            val = val.strip()
            # Key must not be empty
            if not key:
                self.fail(
                    f"Empty key in pair: '{pair}'",
                    param,
                    ctx
                )
            result[key] = val
        return result


class ASlurm(click.RichGroup):
    
    def __init__(self, *args, **kwargs):
        
        super().__init__(*args, **kwargs)
        
        self.rich_logo = RichLogo()
        self.rich_help = RichHelp()
        
        # 0) attribute setup
        
        # This dict will store the global options that are passed to the aslurm base command 
        # such as the name of the config to use etc.
        self.options: dict[str, any] = {}
        
        # 1) registering commands
        # The individual commands are registered
        self.add_command(self.cmd_command)
        
        self.config_group.add_command(self.list_configs_command)
        self.add_command(self.config_group)
        
        # 2) initialization
        # The following section of the constructor performs common initialization tasks which 
        # will be required for all the commands.
        
        #  ~ loading general config
        # This will load the general configuration file that is shipped with the package
        self.aslurm_config: AutoSlurmConfig = AutoSlurmConfig()
        self.aslurm_config.setup_if_necessary()
        
        general_config_path: str = os.path.relpath(
            path=self.aslurm_config.folder_path, 
            start=PATH
        )
        with hydra.initialize(general_config_path, version_base=None):
            cfg = hydra.compose(config_name='general_config')
            cfg_dict = omegaconf.OmegaConf.to_container(cfg, resolve=True, throw_on_missing=True)
            self.general_config: GeneralConfig = GeneralConfig(**cfg_dict)
                    
        # ~ config discovery
        # Here we want to discover all the possible configuration files that are available on the current 
        # system.
        
        # This list will contain the absolute string paths to all of the *folders* containing 
        # viable config files which may be used by the auto-slurm system.
        self.config_source_paths: list[str] = [
            # We put the custom folder first here such that we can use it to override the default configs 
            # that are shipped with the package if we want to.
            self.aslurm_config.configs_folder_path,
            # This is the configs folder that is shipped with the package.
            os.path.join(PATH, 'configs')
        ]
        
        # 3) template environment update
        
        # Add a custom templates folder (e.g., ~/.aslurm/templates) as the highest-priority source
        custom_templates_folder = self.aslurm_config.templates_folder_path
        if os.path.isdir(custom_templates_folder):
            # Prepend the custom loader so its templates override the defaults
            TEMPLATE_ENV.loader = ChoiceLoader([
                FileSystemLoader(custom_templates_folder),
                TEMPLATE_ENV.loader
            ])
    
    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        # Before printing the help text we want to print the logo
        rich.print(self.rich_logo)
        
        rich.print(self.rich_help)
        
        self.format_usage(ctx, formatter)
        self.format_options(ctx, formatter)
        self.format_epilog(ctx, formatter)
    
    # == "config" commands ==
    # Commands to interact with the configuration files.
    
    @click.group('config', short_help='Various commands to interact with the configuration files.')
    @click.pass_obj
    def config_group(self):
        pass
    
    @click.command('list', short_help='List all the configuration files that are available.')
    @click.pass_obj
    def list_configs_command(self):
        """
        Outputs a list of all the available configurations
        """
        # 1) config file discovery
        # This data structure will store the mapping between the name of a config file 
        # and the absolute string path to the file itself.
        config_name_path_dict: dict[str, str] = {}

        for source_path in self.config_source_paths:
            
            try:
                for file_name in os.listdir(source_path):
                    
                    if file_name.endswith('.yaml') or file_name.endswith('.yml'):
                        # We want to use the name of the config file without the extension as the key.
                        config_name = file_name.rsplit('.', 1)[0]
                        # We store the absolute path to the config file as the value.
                        config_name_path_dict[config_name] = os.path.join(source_path, file_name)
            except (FileNotFoundError, NotADirectoryError, OSError):
                continue

        # 2) config file loading
        # In the next step we go through the previous mapping and actually load the content of these 
        # config files into another dictionary.
        
        config_name_content_dict: dict[str, dict] = {}
        for config_name, config_path in config_name_path_dict.items():
            try:
                with open(config_path, 'r') as file:
                    config_content = yaml.safe_load(file)
                config_name_content_dict[config_name] = config_content
            except Exception as e:
                click.echo(f"Failed to load {config_path}: {e}", err=True)
            
        # 3) Display the table
        print()
        rich_config_list = RichConfigList(config_map=config_name_content_dict)
        rich.print(rich_config_list)
    
    # == "cmd" commands ==
    # Commands to actually pass custom things to be scheduled in slurm.
    
    @click.command('cmd',
                   context_settings=dict(ignore_unknown_options=True, allow_extra_args=True), 
                   short_help='Add custom commands to be scheduled in SLURM.')
    @click.argument('args', nargs=-1, type=click.UNPROCESSED)
    @click.pass_obj
    def cmd_command(self, args):
        """
        Add custom commands to be scheduled in SLURM.
        """
        
        # Combine known and unknown args
        args_raw = ['cmd', *args]
        print(args_raw)
        
        # 1) find the individual commands
        # The "cmd" command may actually be composed of multiple "cmd" commands at the same time 
        # and in this first step we want to find all the ones for the given invocation.
        
        # This data structure will store all the individual commands that are found in the 
        # given command line arguments as individual strings.
        commands: list[str] = self.extract_commands_from_args(args_raw)
        click.echo(f'preparing to submit {len(commands)} commands...')
        
        # 2) config loading
        # Here we want to load the config that is specified by the user via the `-cn` option.
        # We search in all of the config source paths for a file with the given name.

        # This method will load the config object based on the given config name from one of the 
        # available config source paths.
        config: Config = self.load_config(config_name=self.options['config_name'])
        click.echo(f'✅ loaded config: {self.options["config_name"]}')
                
        # 3) script folder
        # Next, we need to create the folder where the temporary slurm scripts will actually 
        # be stored in.
        
        # This method will make sure to create a new folder relative to the current working directory 
        # in which we can store the slurm scripts that are generated.
        scripts_path: str = self.create_scipts_folder()
        click.echo(f'✅ created scripts folder @ {scripts_path}')
        
        # 4) job splitting
        
        # With this section we implement the command splitting logic. Either we put all the commands into a 
        # single job / slurm script (if `--same` is set) or we split them into multiple jobs.
        # In this data structure we will store the individual commands lists that will result in individual 
        # slurm scripts. 
        commands_list: list[list[str]] = []
        if self.options['same']:
            commands_list.append(commands)
        else:
            commands_list.extend([[command] for command in commands])
            
        # 5) script generation
        # In this step we will generate the actual slurm scripts based on the commands that we have
        # collected in the previous step.
        
        for job_index, _commands in enumerate(commands_list):
            
            # The filler values that we'll use in the templates to assemble the scripts are 
            # a combination of the global filler values as defaults which are then overwritten 
            # by the values that are specified in the specific config file.
            fillers: dict[str, any] = self.general_config.global_fillers
            fillers.update(config.default_fillers)
            fillers.update(self.options['overwrite_fillers'])
            
            main_content, resume_content = create_slurm_jobs(
                commands=_commands,
                fillers=fillers,
                options=self.options,
                main_template=TEMPLATE_ENV.get_template('main.sh.j2'),
                resume_template=TEMPLATE_ENV.get_template('resume.sh.j2'),
            )
            
            # Now we actually write the files to the scripts folder.
            main_path = os.path.join(scripts_path, f'main_{job_index}.sh')
            with open(main_path, 'w') as main_file:
                main_file.write(main_content)
                
            resume_path = os.path.join(scripts_path, f'resume_{job_index}.sh')
            with open(resume_path, 'w') as resume_file:
                resume_file.write(resume_content)
                
            if not self.options['dry_run']:
                
                try:
                    sbatch_command = ['sbatch', main_path]
                    result = subprocess.run(sbatch_command, capture_output=True, text=True, check=True)

                    output = result.stdout
                    if "Submitted" in output:
                        # Extract the job ID from the output:
                        # The job ID is included in the output as such: "Submitted batch job 123456"
                        job_id: str = output.strip().split()[-1]
                        click.echo(f'🚀 Submitted job {job_index} with SLURM ID {job_id}! - {_commands[0]}...')
                    else:
                        raise ValueError("Unexpected sbatch output format: " + output)
                    
                except (subprocess.CalledProcessError, ValueError) as e:
                    click.echo(f'⚠️ Failed to submit job {job_index}!', err=True)
                    print(e)
                    sys.exit(1)
        
        click.echo()
        click.echo('You may check on the status of your jobs using the `squeue` command.')
        
    # == Helper methods ==
    # The following methods do not implement any commands but rather provide utility functions
    # that are used by the commands above.
        
    def extract_commands_from_args(self, args: list[str]) -> list[str]:
        """
        Extracts individual command strings from a list of arguments, where each command is prefixed by the keyword "cmd".
        This method scans through the provided list of arguments (`args`), searching for occurrences of the string "cmd".
        For each "cmd" found, it collects all subsequent arguments up to the next "cmd" or the end of the list, and joins them
        into a single command string separated by spaces. Each such command string is added to the returned list.
        
        Args:
            args (list[str]): A list of strings representing arguments, where each command is introduced by the keyword "cmd".
                              For example: ["cmd", "echo", "hello", "cmd", "ls", "-l"]
        Returns:
            list[str]: A list of command strings, each assembled from the arguments following a "cmd" keyword up to the next "cmd"
                       or the end of the list. Empty commands (i.e., "cmd" not followed by any arguments) are ignored.
        Example:
            >>> extract_commands_from_args(["cmd", "echo", "hello", "cmd", "ls", "-l"])
            ['echo hello', 'ls -l']
        Notes:
            - If "cmd" appears consecutively (e.g., ["cmd", "cmd", "ls"]), empty commands are ignored.
            - Arguments before the first "cmd" are ignored.
            - The method does not validate the content of the commands, only their extraction based on the "cmd" delimiter.
        """
        # In this list we will store all the individual assembled commands that are found 
        # in the argument list
        commands: list[str] = []
        
        # We iterate through the arguments, as soon as we find a "cmd" argument we start collecting
        # all the following arguments until we find the next "cmd" argument or reach the end of the list.
        # We combine all the arguments in between into a single command string by inserting whitespaces.
        i = 0
        while i < len(args):
            if args[i] == "cmd":
                j = i + 1
                while j < len(args) and args[j] != "cmd":
                    j += 1
                command = " ".join(args[i+1:j])
                if command.strip():
                    commands.append(command)
                i = j
            else:
                i += 1
                
        return commands

    def load_config(self, config_name: str) -> Config:
        """
        Loads a configuration file with the specified name from a list of possible source directories.
        This method iterates over all directories specified in `self.config_source_paths`, attempting to load
        a configuration file matching `config_name` from each location using Hydra. The first successfully
        loaded configuration is returned as an instance of the `Config` class. If no configuration file with
        the given name is found in any of the source paths, a `FileNotFoundError` is raised.
        
        Args:
            config_name (str): The name of the configuration file to load (without file extension).
        Returns:
            Config: An instance of the `Config` class populated with the loaded configuration data.
        Raises:
            FileNotFoundError: If no configuration file with the specified name is found in any of the
                configured source directories.
        Notes:
            - Only the first successfully loaded configuration is used; subsequent paths are not checked.
            - If a configuration is missing in a particular source path, the method silently continues to the
              next path.
        """
        
        # ~ config loading
        # We'll iterate over all of the config source paths and try to load the config with the given 
        # name from each of them until we find one that works.
        config: Config | None = None
        for source_path in self.config_source_paths:
            
            try:
                with hydra.initialize_config_dir(source_path, version_base=None):
                    cfg = hydra.compose(config_name=self.options['config_name'])
                    cfg_dict = omegaconf.OmegaConf.to_container(
                        cfg, resolve=True, throw_on_missing=True
                    )
                    config: Config = Config(**cfg_dict)
                    break
                
            except hydra.errors.MissingConfigException as exc:
                continue

        # If "config" remains None after the loop, that means that no config with the given name was found 
        # in any of the possible locations...
        if config is None:
            raise FileNotFoundError(
                f'There exists no AutoSlurm config file with the name "{args.config}"!. '
                f'Please check the list of available configs...'
            )
            
        return config

    def create_scipts_folder(self) -> str:
        """
        Creates a uniquely named scripts folder within a hidden '.aslurm' directory in the current working directory.
        The folder name is generated using the current date and time (formatted as 'YYYY-MM-DD_HH-MM-SS') 
        concatenated with the first 7 characters of a newly generated UUID, ensuring uniqueness for each invocation.
        
        Returns:
            str: The absolute path to the newly created scripts folder.
        Side Effects:
            - Creates the '.aslurm' directory in the current working directory if it does not already exist.
            - Creates a new subdirectory within '.aslurm' with a unique name.
        Example:
            >>> folder_path = self.create_scipts_folder()
            >>> print(folder_path)
            '/current/working/dir/.aslurm/2024-06-10_15-30-45_1a2b3c4'
        Notes:
            - If the method is called multiple times in quick succession, the UUID ensures that folder names do not collide.
            - The method uses the current working directory as the base path for folder creation.
        """
        
        scripts_folder_path: str = os.path.join(
            os.getcwd(),
            '.aslurm',
            f'{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_{str(uuid.uuid4())[0:7]}'
        )
        os.makedirs(scripts_folder_path, exist_ok=True)
        return scripts_folder_path


@click.group(cls=ASlurm)
@click.option('--config-name', '-cn', help='Config name to use for scheduling.')
@click.option('--overwrite-fillers', '-o', type=KeyValueList(), default={}, help='overwrite fillers for the config.')
@click.option('--same', '-s', is_flag=True, help='Put all the commands into the same job.')
@click.option('--gpus-per-task', '-gpt', type=int, default=None, help='Number of GPUs per task.')
@click.option('--num-gpus', '-ng', type=int, default=None, help='Number of GPUs to use in total. If not set, will use all available GPUs.')
@click.option('--dry-run', '-d', is_flag=True, help='Do not actually submit the jobs, just print the commands that would be run.')
@click.option('--version', '-v', is_flag=True, help='Show the version.')
@click.pass_context
def aslurm(ctx: click.Context,
           config_name: str,
           overwrite_fillers: dict,
           same: bool,
           gpus_per_task: int | None,
           num_gpus: int | None,
           dry_run: bool,
           version: bool,
           ) -> None:        

    ctx.obj = ctx.command
    options = {
        'config_name':          config_name,
        'overwrite_fillers':    overwrite_fillers,
        'same':                 same,
        'gpus_per_task':        gpus_per_task, 
        'num_gpus':             num_gpus,
        'dry_run':              dry_run,
        'version':              version
    }
    ctx.command.options.update(options)
    
    if version:
        version_string: str = get_version()
        click.echo(version_string)
        sys.exit(0)
        

if __name__ == '__main__':
    aslurm()