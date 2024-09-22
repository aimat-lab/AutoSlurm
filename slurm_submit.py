import argparse
import yaml

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Slurm submit helper."
    )

    parser.add_argument(
        "config",
        type=str,
    )

    args = parser.parse_args()

    config_file_path = "configs/" + args.config + ".yaml"
    config_file = open(config_file_path, "r")
    config = yaml.safe_load(config_file)
    config_file.close()

    print(repr(config))