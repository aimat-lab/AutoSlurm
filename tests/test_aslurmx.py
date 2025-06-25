import os
import pytest
import tempfile
from click.testing import CliRunner
from auto_slurm.aslurmx import aslurm
from auto_slurm.helpers import get_version


class TestAslurmX:
    
    def test_aslurmx_help_command(self):
        runner = CliRunner()
        result = runner.invoke(aslurm, ['--help'])
        assert result.exit_code == 0
        assert 'Options' in result.output
        assert 'Commands' in result.output
        assert 'AutoSlurm Command Line Interface' in result.output
        
    def test_aslurmx_version_command(self):
        
        version_true = get_version()
        
        runner = CliRunner()
        result = runner.invoke(aslurm, ['--version'])
        assert result.exit_code == 0
        assert version_true in result.output
        
    @pytest.mark.parametrize('config', ['haicore_1gpu', 'haicore_4gpu'])
    def test_aslurmx_generally_works(self, config: str):
        
        tasks = ["cmd", "python -c 'print(True)'"]
        
        with tempfile.TemporaryDirectory() as temp_path:
            
            runner = CliRunner()
            result = runner.invoke(aslurm, [
                f'--archive-path={temp_path}',
                f'--config-name={config}',
                '-d',
                'cmd', 'python -c "print(True)"'
            ])
            assert result.exit_code == 0, f"Command failed: {result.output}"
        
    @pytest.mark.parametrize('max_tasks', [2, ])
    def test_aslurmx_max_commands_works(self, max_tasks: int):
        
        multiplier = 2
        num_tasks = max_tasks * multiplier
        tasks = ["cmd", "python -c 'print(True)'"] * num_tasks
        
        with tempfile.TemporaryDirectory() as temp_path:
            
            runner = CliRunner()
            result = runner.invoke(aslurm, [
                f'--archive-path={temp_path}',
                '--config-name=haicore_1gpu',
                '-d',
                '-mt', str(max_tasks),
                *tasks
            ])
            assert result.exit_code == 0, f"Command failed: {result.output}"
            
            aslurm_path = os.path.join(temp_path, '.aslurm')
            assert os.path.exists(aslurm_path), "aslurm directory was not created."
            sh_files = [
                os.path.join(root, file)
                for root, _, files in os.walk(aslurm_path)
                for file in files if file.endswith('.sh')
            ]
            # Each task creates a main and a resume script
            assert len(sh_files) == multiplier * 2
        
        