from os import path, fsync
from sys import argv
from datetime import datetime
from multiprocessing import Process
from math import ceil

import gc

def count_offset(fpath, number):
    file_size = path.getsize(fpath)
    each_size = ceil(file_size / number)
    offsets = [0]

    with open(fpath, 'rb') as fptr:
        for i in range(1,number):
            fptr.seek(each_size, 1)
            fptr.readline()
            offsets.append(fptr.tell())
    
    offsets.append(file_size)
    print(offsets)

    return offsets

def handle(current_offset, next_offset, path):
    with open(path, 'r', encoding='utf8') as fptr:
        fptr.seek(current_offset)

        max_page_size = 1000
        current_page_size = 0

        output_buffer = {}
        keys = []

        while True:
            if fptr.tell() >= next_offset:
                print(current_offset, fptr.tell(), next_offset)
                for f_key in keys:
                    output_files[f_key].write(''.join(output_buffer[f_key]))
                return True

            line = fptr.readline()
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

                for f_key in keys:
                    output_files[f_key].write(''.join(output_buffer[f_key]))

                del output_buffer
                del keys

                gc.collect()
                
                output_buffer = {}
                keys = []
                
            current_page_size += 1

if __name__ == '__main__':
    gc.enable()

    base_path = path.dirname(path.realpath(__file__))

    output_files = {}
    target_file_path = argv[1]
    decision_column_index = int(argv[2])
    
    process_number = 4
    process_list = []

    offsets = count_offset(target_file_path, process_number)

    start = datetime.now()

    for i in range(process_number):
        process_list.append(Process(target=handle, args=(offsets[i], offsets[i + 1], target_file_path)))
        process_list[i].start()
    
    for process in process_list:
        process.join()

    for fptr in output_files:
        fptr.flush()
        fsync(fptr.fileno())
        
        output_files[key].close()
    
    print('Speed Time: ', datetime.now() - start)

    gc.collect()