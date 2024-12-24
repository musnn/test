import sys
import csv
import json
from collections import defaultdict
import statistics
import os

def parse_results(output_dir, search_file_name):
    search_file = os.path.join(output_dir, search_file_name)
    output_path_parsed = search_file.replace("_search.tsv", ".parsed")
    
    cath_ids = defaultdict(int)
    plDDT_values = []
    
    with open(search_file, "r") as file:
        next(file)  # Skip header
        reader = csv.reader(file, delimiter='\t')
        for row in reader:
            plDDT_values.append(float(row[3]))
            meta = json.loads(row[15])
            cath_ids[meta["cath"]] += 1
    
    with open(output_path_parsed, "w", newline='', encoding="utf-8") as file:
        file.write(f"# mean plddt: {statistics.mean(plDDT_values) if plDDT_values else 0}\n")
        for cath_id, count in cath_ids.items():
            file.write(f"{cath_id},{count}\n")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python results_parser.py <output_directory> <search_tsv_filename>")
        sys.exit(1)
    parse_results(sys.argv[1], sys.argv[2])
