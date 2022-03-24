import csv
import sys


if __name__ == "__main__":
    sex, input_file, output_file = sys.argv[1:]
    with open(input_file) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    with open(output_file, "w") as f:
        writer = csv.DictWriter(f, rows[0].keys())
        writer.writeheader()
        for row in rows:
            if row["sex"] == sex:
                writer.writerow(row)
