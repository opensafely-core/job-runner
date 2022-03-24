import argparse
import json


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    parser.add_argument("output_file")
    args = parser.parse_args()

    config = json.loads(args.config)
    counts = {}
    for input_file in config["files"]:
        with open(input_file) as f:
            counts[input_file] = len(f.readlines())
    with open(args.output_file, "w") as f:
        for name, count in counts.items():
            f.write(f"{name}: {count}\n")
