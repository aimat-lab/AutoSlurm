import os
import pathlib
import shutil

from typing import Dict
from pydantic import BaseModel, ConfigDict
from pydantic import PositiveInt
from pydantic import model_validator
from typing_extensions import Self
from typing import Optional
from appdirs import user_config_dir, user_cache_dir

# This is the absolute string path to the auto-slurm PACKAGE folder which 
# has been installed in the Python environment. This will be the path which 
# contains the base version of the general_config.yaml file...
PATH: str = pathlib.Path(__file__).parent.absolute()


class GeneralConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    hostname_config_mappings: Dict[str, str]
    global_fillers: Dict[str, str]


class Config(BaseModel):
    
    model_config = ConfigDict(extra="forbid")

    template: str
    default_fillers: Dict[str, Optional[str]]

    NO_gpus: Optional[PositiveInt]
    max_tasks: Optional[PositiveInt]

    gpus_per_task: Optional[PositiveInt]

    @model_validator(mode="after")
    def check(self) -> Self:
        if self.NO_gpus is not None:
            assert (
                self.max_tasks is None
            ), "NO_gpus and max_tasks cannot be set at the same time"
            assert (
                self.gpus_per_task is not None
            ), "NO_gpus and gpus_per_task should be set at the same time"

        if self.max_tasks is not None:
            assert (
                self.NO_gpus is None
            ), "NO_gpus and max_tasks cannot be set at the same time"
            assert (
                self.gpus_per_task is None
            ), "gpus_per_task and max_tasks cannot be set at the same time"

        return self



class AutoSlurmConfig:
    """
    AutoSlurmConfig is a class that handles the configuration of the AutoSlurm application.
    It is responsible for setting up the configuration folder including, for example, the
    coyping of the general_config.yaml file.
    """
    
    app_name: str = 'auto_slurm'
    app_author: str = 'AIMAT'
    
    def __init__(self, folder_path: Optional[str] = None):
        # "user_config_dir" will return the absolute string path to the ".config/auto-slurm" 
        # folder on in the users home directory for Linux and MacOS. The cool thing is that it 
        # will also work for Windows, but the path will be different...
        self.folder_path: str = user_config_dir(
            self.app_name, 
            self.app_author
        )
        
        # Optional overwrite for unittesting purposes
        if folder_path is not None:
            self.folder_path = folder_path
        
        # This will be the path to the "configs" subfolder in our own config folder where 
        # the use can define local custom config files for the aslurm command.
        self.configs_folder_path: str = os.path.join(self.folder_path, 'configs')
        
        # This will be the path to the "templates" subfolder in our own config folder.
        # We can use this folder to overwrite the default jinja2 templates for the 
        # slurm scripts for example.
        self.templates_folder_path: str = os.path.join(self.folder_path, 'templates')
    
    def setup_if_necessary(self) -> None:
        """
        Checks if the .config/auto-slurm folder exists. If it does not exist, we create it and
        copy the general_config.yaml file from the package folder to the config folder.
        
        Returns:
            None
        """
        
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
            self.setup()
    
    def setup(self) -> None:
        """
        Sets up all the necessary structure within our own .config/auto_slurm folder. This 
        included the copying the general_config.yaml file from the package folder to the config
        folder.
        
        Returns:
            None
        """
        
        # ~ copy the general config
        # The first thing that we need to do to setup our own config folder is to copy the 
        # general_config.yaml file from the package folder to the config folder. 
        shutil.copy(
            os.path.join(PATH, 'general_config.yaml'), 
            os.path.join(self.folder_path, 'general_config.yaml')
        )
        
        # ~ create the "configs" folder
        # Additionally we want to create a "configs" folder within our own config folder 
        # that the user can use to store their own local config files for the aslurm command.
        # We'll configure Hydra to also look inside this folder!
        os.makedirs(self.configs_folder_path, exist_ok=True)
        
        # ~ create the "templates" folder
        # We also want to create a "templates" folder within our own config folder that the
        # user can use to store their own local jinja2 templates for the slurm scripts.
        os.makedirs(self.templates_folder_path, exist_ok=True)
        
        # ~ copy the "main" base config
        # We need to copy the "main" base config file from the package folder to the configs 
        # folder that was just created so that custom configs can inherit from it.
        shutil.copy(
            os.path.join(PATH, 'configs', 'main.yaml'), 
            os.path.join(self.configs_folder_path, 'main.yaml')
        )
        