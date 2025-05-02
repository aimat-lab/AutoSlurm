"""
Unittests for the "auto_slurm/testing.py" module
"""

import os
from auto_slurm.config import AutoSlurmConfig
from auto_slurm.testing import MockAutoSlurmConfig


class TestMockAutoSlurmConfig:
    
    def test_basically_works(self):
        
        with MockAutoSlurmConfig() as config:
            
            assert isinstance(config, AutoSlurmConfig)
            
            # The path itself should exist
            assert os.path.exists(config.folder_path)
            # The general config should be copied
            assert os.path.exists(os.path.join(config.folder_path, 'general_config.yaml'))
            # The configs folder path should exist
            assert os.path.exists(config.configs_folder_path)