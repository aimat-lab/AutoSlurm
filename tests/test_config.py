"""
Unittests for the "auto_slurm/config.py" module
"""

import os
import tempfile
from auto_slurm.config import AutoSlurmConfig


class TestAutoSlurmConfig:
    
    def test_setup_basically_works(self):
        
        with tempfile.TemporaryDirectory() as path:
            
            config_path = os.path.join(path, 'auto_slurm')
            config = AutoSlurmConfig(folder_path=config_path)
            config.setup_if_necessary()
            
            # Check if the folder was created
            assert os.path.exists(config.folder_path)
            
            # Check if the general_config.yaml file was copied
            assert os.path.exists(os.path.join(config.folder_path, 'general_config.yaml'))
            
            # Check if the configs folder was created
            assert os.path.exists(config.configs_folder_path)
            
            # Check if the configs folder is empty
            assert len(os.listdir(config.configs_folder_path)) == 1
            