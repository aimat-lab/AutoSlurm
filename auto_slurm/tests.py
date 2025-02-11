import unittest
from auto_slurm.aslurm import (
    build_commands_str,
    expand_commands,
    create_slurm_job_files,
)


class TestSlurmScript(unittest.TestCase):

    def test_expand_commands_brackets(self):
        commands = ["python train.py --lr <[0.01,0.1]> --batch_size <[32,64]>"]
        expected = [
            "python train.py --lr 0.01 --batch_size 32",
            "python train.py --lr 0.1 --batch_size 64",
        ]
        self.assertEqual(expand_commands(commands), expected)

    def test_expand_commands_braces(self):
        commands = ["python train.py --lr <{0.01,0.1}> --batch_size <{32,64}>"]
        expected = [
            "python train.py --lr 0.01 --batch_size 32",
            "python train.py --lr 0.01 --batch_size 64",
            "python train.py --lr 0.1 --batch_size 32",
            "python train.py --lr 0.1 --batch_size 64",
        ]
        self.assertEqual(expand_commands(commands), expected)

    def test_build_commands_str(self):
        commands = ["python train.py --lr 0.01", "python train.py --lr 0.1"]
        job_start_task_index = 0
        gpus_per_task = 1

        command_str = build_commands_str(commands, job_start_task_index, gpus_per_task)

        self.assertIn(
            "SLURM_SUBMIT_TASK_INDEX=0 CUDA_VISIBLE_DEVICES=0 python train.py --lr 0.01",
            command_str,
        )
        self.assertIn(
            "SLURM_SUBMIT_TASK_INDEX=1 CUDA_VISIBLE_DEVICES=1 python train.py --lr 0.1",
            command_str,
        )

    def test_create_slurm_job_files(self):
        template = "#SBATCH --job-name=<job_name>\nsomecommand > <output_file>"
        fillers = {"job_name": "<inner_filler>", "output_file": "test.out"}
        global_fillers = {"inner_filler": "test_job"}

        create_slurm_job_files(
            "main.sh",
            "resume.sh",
            0,
            template,
            fillers,
            global_fillers,
            commands=["echo test0", "echo test1"],
            gpus_per_task=2,
        )

        # Read the files:
        with open("main.sh", "r") as f:
            main_contents = f.read()
        with open("resume.sh", "r") as f:
            resume_contents = f.read()

        self.assertIn("#SBATCH --job-name=test_job", main_contents)
        self.assertIn("#SBATCH --job-name=test_job", resume_contents)
        self.assertIn("echo test0", main_contents)
        self.assertIn("echo test1", main_contents)
        self.assertIn("eval", resume_contents)
        self.assertIn("CUDA_VISIBLE_DEVICES=0,1", main_contents)
        self.assertIn("CUDA_VISIBLE_DEVICES=0,1", resume_contents)
        self.assertIn("CUDA_VISIBLE_DEVICES=2,3", main_contents)
        self.assertIn("CUDA_VISIBLE_DEVICES=2,3", resume_contents)

    def test_expand_commands_invalid_syntax(self):
        commands = ["python train.py --lr <[0.01,0.1]> --batch_size <{32,64}>"]
        with self.assertRaises(ValueError):
            expand_commands(commands)

        commands = ["python train.py --lr <[0.01,0.1,1.0]> --batch_size <[32,64]>"]
        with self.assertRaises(ValueError):
            expand_commands(commands)

    def test_build_commands_no_gpus(self):
        commands = ["python train.py --lr 0.01"]
        command_str = build_commands_str(commands, 0, None)
        self.assertNotIn("CUDA_VISIBLE_DEVICES", command_str)


if __name__ == "__main__":
    unittest.main()
