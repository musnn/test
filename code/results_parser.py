import sys
import csv
import json
from collections import defaultdict
import statistics
import os

def parse_results(input_dir, search_file):
    cath_ids = defaultdict(int)
    plDDT_values = []

    # Construct full path for input and output
    input_path = os.path.join(input_dir, search_file)
    output_path_parsed = os.path.join(input_dir, search_file.replace("_search.tsv", ".parsed"))
    output_path_csv = os.path.join(input_dir, search_file.replace("_search.tsv", "_summary.csv"))
    output_path_plDDT = os.path.join(input_dir, "plDDT_means.csv")

    with open(input_path, "r") as fhIn:
        next(fhIn)  # Skip header
        msreader = csv.reader(fhIn, delimiter='\t') 
        for row in msreader:
            plDDT_values.append(float(row[3]))
            meta = json.loads(row[15])
            cath_ids[meta["cath"]] += 1

    mean_plddt = statistics.mean(plDDT_values) if plDDT_values else 0
    std_plddt = statistics.stdev(plDDT_values) if len(plDDT_values) > 1 else 0

    # Write parsed output
    with open(output_path_parsed, "w", encoding="utf-8") as fhOut:
        fhOut.write(f"#{search_file} Results. mean plddt: {mean_plddt}\n")
        fhOut.write("cath_id,count\n")
        for cath, v in cath_ids.items():
            fhOut.write(f"{cath},{v}\n")

    # Write CSV output for cath_ids
    with open(output_path_csv, "w", newline='', encoding="utf-8") as fhOut:
        writer = csv.writer(fhOut)
        writer.writerow(["cath_id", "count"])
        for cath, count in cath_ids.items():
            writer.writerow([cath, count])

    # Write plDDT means and std
    with open(output_path_plDDT, "a", newline='', encoding="utf-8") as fhOut:
        writer = csv.writer(fhOut)
        if os.stat(output_path_plDDT).st_size == 0:  # Check if file is empty to write headers
            writer.writerow(["organism", "mean_plddt", "plddt_std"])
        organism = os.path.basename(input_dir)  # This should be set to a meaningful value
        writer.writerow([organism, f"{mean_plddt:.3f}", f"{std_plddt:.3f}"])

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python results_parser.py <output_directory> <search_tsv_filepath>")
        sys.exit(1)
    parse_results(sys.argv[1], sys.argv[2])
