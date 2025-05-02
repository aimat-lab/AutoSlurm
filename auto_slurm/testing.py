import os
import tempfile
from typing import Optional

from auto_slurm.config import AutoSlurmConfig




class MockAutoSlurmConfig:
    """
    This is a context manager that creates a temporary directory and sets up a new 
    AutoSlurmConfig object in it. The temporary directory will be deleted when the 
    context manager is exited. This is useful for testing purposes, as it allows 
    to create a clean environment for each test without affecting the user's
    configuration files.
    """
    
    def __init__(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        
        # This will hold the absolute string path to the temporary directory
        self.temp_path: Optional[str] = None
        # This will hold the absolute string path to the "auto_slurm" sub folder in 
        # the temporary directory
        self.config_path: Optional[str] = None
        # After initialization, this will hold the AutoSlurmConfig object that will 
        # be created in the temporary directory and which manages the configuration
        self.config: Optional[AutoSlurmConfig] = None
    
    def __enter__(self) -> AutoSlurmConfig:
        self.temp_path = self.temp_dir.__enter__()
        self.config_path = os.path.join(self.temp_path, 'auto_slurm')
        self.config = AutoSlurmConfig(folder_path=self.config_path)
        self.config.setup_if_necessary()
        
        return self.config
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.temp_dir.__exit__(exc_type, exc_value, traceback)
        