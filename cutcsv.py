from os import path, fsync
from sys import argv
from datetime import datetime

import gc

def estimate_lines(fpath, ignore_lines = 0):
    file_size = path.getsize(fpath)
    sample_size = 0
    sampling_time = 10

    fptr = open(fpath, 'rb')

    for _ in range(sampling_time):
        sample_size += len(fptr.readline())

    fptr.close()

    return int(file_size / (sample_size / sampling_time))

def main():
    target_file_path = argv[1]
    decision_column_index = int(argv[2])

    output_files = {}

    with open(target_file_path, 'r', encoding='utf8') as f:
        keys = []
        output_buffer = {}

        max_page_size = 500
        current_page_size = 0

        for line in f:
            key = line.split(',')[decision_column_index]

            if key in keys:
                output_buffer[key].append(line)
            else:
                if key not in output_files.keys():
                    output_files[key] = open(base_path + '/' + key + '.csv', 'a', encoding='utf8')
                keys.append(key)
                output_buffer[key] = [line]

            if current_page_size >= max_page_size:
                current_page_size = 0

                for key in keys:
                    output_files[key].write('\n'.join(output_buffer[key]))

                del output_buffer
                del keys

                gc.collect()
                
                output_buffer = {}
                keys = []
                
            current_page_size += 1
    
    for key in keys:
        output_files[key].write('\n'.join(output_buffer[key]))

        output_files[key].flush()
        fsync(output_files[key].fileno())
        
        output_files[key].close()

if __name__ == '__main__':
    
    gc.enable()

    base_path = path.dirname(path.realpath(__file__))

    # start = datetime.now()
    main()
    # print(datetime.now() - start)

    gc.collect()

    # print(timeit.timeit(main, number=6))
    # cProfile.run('main()')
