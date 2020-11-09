import sys

if __name__ == "__main__":
    output_file, *input_files = sys.argv[1:]
    counts = {}
    for input_file in input_files:
        with open(input_file) as f:
            counts[input_file] = len(f.readlines())
    with open(output_file, "w") as f:
        for name, count in counts.items():
            f.write(f"{name}: {count}\n")
