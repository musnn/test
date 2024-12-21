import sys
from subprocess import Popen, PIPE
import glob
import os
import multiprocessing

"""
usage: python pipeline_script.py [INPUT DIR] [OUTPUT DIR]
approx 5seconds per analysis
"""

def run_parser(input_file, output_dir):
    """
    Run the results_parser.py over the hhr file to produce the output summary
    """
    search_file = input_file+"_search.tsv"
    print(search_file, output_dir)
        
    full_path_to_search_file = os.path.join(output_dir, search_file)
    print(f"Full path to search file: {full_path_to_search_file}")

    cmd = ['python', './results_parser.py', output_dir, search_file]
    print(f'STEP 2: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    
    if p.returncode != 0:
        print("Error running command:", ' '.join(cmd))
        print("stdout:", out.decode())
        print("stderr:", err.decode())
    print(out.decode("utf-8"))


def run_merizo_search(input_file, id):
    """
    Runs the merizo domain predictor to produce domains
    """
    output_directory = '/home/almalinux/test/result'
    database_directory = '/home/almalinux/test/cath_foldclassdb/cath-4.3-foldclassdb'
    merizo_executable = '/home/almalinux/merizo_search/merizo_search/merizo.py'
    cmd = ['python3',
           '/home/almalinux/merizo_search/merizo_search/merizo.py',
           'easy-search',
           os.path.abspath(input_file),
           '/home/almalinux/test/cath_foldclassdb/cath-4.3-foldclassdb',
           os.path.join(output_directory, id),
           'tmp',
           '--iterate',
           '--output_headers',
           '-d',
           'cpu',
           '--threads',
           '1'
           ]
    print(f'STEP 1: RUNNING MERIZO: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    print("Merizo stdout:", out.decode())
    print("Merizo stderr:", err.decode())
    if p.returncode != 0:
        print("Merizo search failed")
    else:
        print("Merizo search completed successfully")
    print("Listing files in output directory:")
    print(os.listdir('/home/almalinux/test/result/'))
def read_dir(input_dir):
    """
    Function reads a fasta formatted file of protein sequences
    """
    print("Getting file list")
    file_ids = list(glob.glob(input_dir+"*.pdb"))
    analysis_files = []
    for file in file_ids:
        id = file.rsplit('/', 1)[-1]
        analysis_files.append([file, id, sys.argv[2]])
    return(analysis_files)

def pipeline(filepath, id, outpath):
    # STEP 1
    run_merizo_search(filepath, id)
    # STEP 2
    run_parser(id, outpath)

if __name__ == "__main__":
    pdbfiles = read_dir(sys.argv[1])
    p = multiprocessing.Pool(1)
    p.starmap(pipeline, pdbfiles[:10])

        
