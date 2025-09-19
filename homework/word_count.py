"""Taller evaluable"""

import glob
import os
import string
import time


def prepare_input_directory(input_dir):
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
    else:
        for file in glob.glob(f"{input_dir}/*"):
            os.remove(file)


def generate_file_copies(n, raw_dir, input_dir):
    for file in glob.glob(f"{raw_dir}/*"):
        with open(file, "r", encoding="utf-8") as f:
            text = f.read()

        raw_filename_with_extension = os.path.basename(file)
        raw_filename_without_extension = os.path.splitext(raw_filename_with_extension)[
            0
        ]

        for i in range(n):
            new_filename = f"{raw_filename_without_extension}_{i}.txt"
            with open(f"{input_dir}/{new_filename}", "w", encoding="utf-8") as f2:
                f2.write(text)


def mapper(sequence):
    pairs_sequence = []
    for _, line in sequence:
        line = line.lower()
        line = line.translate(str.maketrans("", "", string.punctuation))
        line = line.replace("\n", "")
        words = line.split()
        pairs_sequence.extend([(word, 1) for word in words])
    return pairs_sequence


def reducer(pairs_sequence):
    result = []
    for key, value in pairs_sequence:
        if result and result[-1][0] == key:
            result[-1] = (key, result[-1][1] + value)
        else:
            result.append((key, value))
    return result


def hadoop(input_dir, output_dir, mapper_func, reducer_func):

    def emit_input_lines(input_dir):
        sequence = []
        files = glob.glob(f"{input_dir}/*")
        for file in files:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    sequence.append((file, line))
        return sequence

    def shuffle_and_sort(pairs_sequence):
        pairs_sequence = sorted(pairs_sequence)
        return pairs_sequence

    def create_output_folder(output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        else:
            raise FileExistsError(f"The folder {output_dir} already exists")

    def write_results_to_file(result, output_dir):
        with open(f"{output_dir}/part-00000", "w", encoding="utf-8") as f:
            for key, value in result:
                f.write(f"{key}\t{value}\n")

    def create_success_file(output_dir):
        with open(f"{output_dir}/_SUCCESS", "w", encoding="utf-8") as f:
            f.write("")

    sequence = emit_input_lines(input_dir)
    pairs_sequence = mapper_func(sequence)
    pairs_sequence = shuffle_and_sort(pairs_sequence)
    result = reducer_func(pairs_sequence)
    create_output_folder(output_dir)
    write_results_to_file(result, output_dir)
    create_success_file(output_dir)


def run_experiment(n):

    # Define directories for raw, input, and output files
    raw_dir = "files/raw/"
    input_dir = "files/input/"
    output_dir = "files/output/"

    # delete the output folder if it exists
    if os.path.exists("files/output/"):
        for file in os.listdir("files/output/"):
            os.remove(os.path.join("files/output/", file))
        os.rmdir("files/output/")

    # Clear the input directory to ensure a clean start
    prepare_input_directory(input_dir)

    # Generate 'n' copies of each file from the raw directory into the input directory
    generate_file_copies(n, raw_dir, input_dir)

    # Record the start time of the experiment
    start_time = time.time()

    # Run the MapReduce process using the input and output directories
    hadoop(input_dir, output_dir, mapper, reducer)

    # Record the end time of the experiment
    end_time = time.time()

    # Print the total execution time
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")


if __name__ == "__main__":

    run_experiment(10000)

    
    