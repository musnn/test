import sys
from subprocess import Popen, PIPE
import glob
import os
import multiprocessing

def run_parser(output_dir, input_file):
    """
    Run the results_parser.py over the output file to produce the final summary
    """
    search_file = os.path.join(output_dir, input_file + "_search.tsv")
    print(f'Running parser on {search_file}')
    cmd = ['python', 'results_parser.py', output_dir, search_file]
    print(f'STEP 2: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    print(out.decode("utf-8"))
    if err:
        print("Parser Error:", err.decode())

def run_merizo_search(input_file, output_dir, id):
    """
    Runs the merizo domain predictor to produce domains
    """
    cmd = ['python3',
           '/home/almalinux/merizo_search/merizo_search/merizo.py',
           'easy-search',
           input_file,
           '/home/almalinux/data/cath-4.3-foldclassdb',
           os.path.join(output_dir, id),
           'tmp',
           '--iterate',
           '--output_headers',
           '-d',
           'cpu',
           '--threads',
           '1']
    print(f'STEP 1: RUNNING MERIZO: {" ".join(cmd)}')
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if err:
        print("Merizo Error:", err.decode())

def read_dir(input_dir):
    """
    Function reads a fasta formatted file of protein sequences
    """
    print("Getting file list")
    file_ids = list(glob.glob(os.path.join(input_dir, "*.pdb")))
    analysis_files = []
    for file in file_ids:
        id = os.path.basename(file).replace('.pdb', '')
        analysis_files.append([file, id])
    return analysis_files

def pipeline(filepath, id, outpath):
    # STEP 1: Run Merizo search
    run_merizo_search(filepath, outpath, id)
    # STEP 2: Parse the results
    run_parser(outpath, id)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python pipeline_script.py [INPUT DIR] [OUTPUT DIR]")
        sys.exit(1)
    input_dir, output_dir = sys.argv[1], sys.argv[2]
    pdbfiles = read_dir(input_dir)
    p = multiprocessing.Pool(1)  # Adjust number of processes as necessary
    args = [(filepath, id, output_dir) for filepath, id in pdbfiles]
    p.starmap(pipeline, args)
