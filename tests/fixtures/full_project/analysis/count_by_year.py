import collections
import csv
import sys


def main(input_file, output_file):
    with open(input_file, newline="") as f_in:
        csv_reader = csv.DictReader(f_in)
        counter = collections.Counter(int(x["year_of_birth"]) for x in csv_reader)

    with open(output_file, "w", newline="") as f_out:
        csv_writer = csv.writer(f_out)
        csv_writer.writerow(["year", "count"])
        csv_writer.writerows(x for x in sorted(counter.items(), key=lambda x: x[0]))


if __name__ == "__main__":
    main(*sys.argv[1:])
