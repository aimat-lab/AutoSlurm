# Slurm submission helper

<span style="color:red">*This code is under active development, not useable yet.*</span>.

This submission helper allows you to automatically generate slurm submission scripts and start them for you.
Supports simple jobs, splitting multiple scripts accross the GPUs of a node, and even chain jobs - all
with a single command.

```
function slurm_submit() {
    python /path/to/repository/slurm_submit.py "$@"
}
```