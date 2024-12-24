import sys
import csv
import json
from collections import defaultdict
import statistics
import os

def parse_results(search_file):
    output_path_parsed = search_file.replace("_search.tsv", ".parsed")
    
    cath_ids = defaultdict(int)
    plDDT_values = []
    
    try:
        with open(search_file, "r") as file:
            next(file)  # Skip header
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                plDDT_value = float(row[3])
                plDDT_values.append(plDDT_value)
                meta = json.loads(row[15])
                cath_ids[meta["cath"]] += 1
        
        mean_plddt = statistics.mean(plDDT_values) if plDDT_values else 0

        # Only write parsed file if there is meaningful data
        if mean_plddt > 0 and cath_ids:  # Checks if mean plDDT is greater than zero and cath_ids is not empty
            with open(output_path_parsed, "w", newline='', encoding="utf-8") as file:
                file.write(f"# mean plddt: {mean_plddt}\n")
                for cath_id, count in cath_ids.items():
                    file.write(f"{cath_id},{count}\n")
        else:
            print(f"No meaningful data found in {search_file}. No parsed file created.")

    except FileNotFoundError:
        print(f"File not found: {search_file}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python results_parser.py <search_tsv_filepath>")
        sys.exit(1)
    parse_results(sys.argv[1])
