"""Taller evaluable"""
import os,glob,time,string

#Crear la carperta folder/input donde voy a copiar los archivos raw
def clear_input_directory(input_dir):
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
    else:
        for file in glob.glob(f"{input_dir}/*"):
            os.remove(file)

#Copiar n veces los archivos raw a la carpeta input
def generate_input_files( n, raw_dir, input_dir):
    # Archivos hay en la carpeta raw
    for file in glob.glob(f"{raw_dir}/*.txt"):
        with open(file, 'r', encoding="utf-8") as f:
            text = f.read()

        for i in range(n):
            raw_filename_with_extension = os.path.basename(file)
            raw_filename_without_extension = os.path.splitext(raw_filename_with_extension)[0]
            new_filename = f"{raw_filename_without_extension}_{i}.txt"

            with open(f"{input_dir}/{new_filename}", "w", encoding="utf-8") as f2:
                f2.write(text)

# Mapea las líneas a pares (palabra, 1). Este es el mapper.
def mapper(sequence):
    pairs_sequence = []
    for _, line in sequence:
        line = line.lower()
        line = line.translate(str.maketrans("", "", string.punctuation))
        line = line.replace("\n", "")
        words = line.split()
        pairs_sequence.extend([(word, 1) for word in words])
    return pairs_sequence


#Reduce los pares sumando los valores por cada palabra. Este es el reducer.
def reducer(pairs_sequence):
    result = []
    for key, value in pairs_sequence:
        if result and result[-1][0] == key:
            result[-1] = (key, result[-1][1] + value)
        else:
            result.append((key, value))
    return result


def hadoop(input_dir, output_dir, mapper_func, reducer_func):
    
    #Lee los archivos de file/input
    def emit_input_lines(input_dir):
        sequence = []
        files = glob.glob(f"{input_dir}/*.txt")
        for file in files:
            with open(file, 'r', encoding="utf-8") as f:
                for line in f:
                    sequence.append((file, line))
        return sequence   

    # Ordena la secuencia de pares por la palabra. Este es el shuffle and sort.
    def shuffle_and_sort(pairs_sequence):
        pairs_sequence = sorted(pairs_sequence)
        return pairs_sequence

    def create_output_folder(output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        else:
            #generar error si la carpeta ya existe
            raise FileExistsError(f"La carpeta {output_dir} ya existe")
        
    # Guardar el resultado en un archivo files/output/part-00000
    def write_results_to_file(result, output_dir):
        with open(f"{output_dir}/part-00000", "w", encoding="utf-8") as f:
            for key, value in result:
                f.write(f"{key}\t{value}\n")
    
    # Guardar el archivo _SUCCESS en la carpeta output
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
    input_dir = "files/input"
    output_dir = "files/output"
    raw_dir = "files/raw"
    clear_input_directory(input_dir)
    generate_input_files(n, raw_dir, input_dir)
    start_time = time.time() #El experimento realmente empieza aqui
    hadoop(input_dir, output_dir, mapper, reducer)
    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")


run_experiment(1000)