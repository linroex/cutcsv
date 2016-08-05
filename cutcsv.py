from os import path, fsync
from sys import argv
from datetime import datetime
from multiprocessing import Process
from math import floor, ceil

import gc

def count_offset(fpath, number):
    each_size = int(path.getsize(fpath) / number)
    offsets = []

    with open(fpath, 'rb') as fptr:
        for i in range(number):
            fptr.seek(each_size * i)
            if i != 0:
                fptr.readline()
            offsets.append(fptr.tell())
    
    return offsets

def handle(index, process_bytes, path):
    with open(path, 'r', encoding='utf8') as fptr:
        fptr.seek(int(index * process_bytes))
        fptr.readline()

        max_page_size = 1000
        current_page_size = 0
        output_buffer = {}
        keys = []

        for i in range(int(process_bytes)):
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

                for key in keys:
                    output_files[key].write(''.join(output_buffer[key]))

                del output_buffer
                del keys

                gc.collect()
                
                output_buffer = {}
                keys = []
                
            current_page_size += 1

            if fptr.tell() >= process_bytes*(index+1):
                for key in keys:
                    output_files[key].write(''.join(output_buffer[key]))
                return True
        
if __name__ == '__main__':
    gc.enable()

    base_path = path.dirname(path.realpath(__file__))

    output_files = {}

    target_file_path = argv[1]
    decision_column_index = int(argv[2])

    process_number = 5
    process_list = []
    process_bytes = path.getsize(target_file_path) / process_number

    start = datetime.now()

    offsets = count_offset(target_file_path, process_number)

    for i in range(process_number):
        process_list.append(Process(target=handle, args=(i, process_bytes, target_file_path, )))
        process_list[i].start()
    
    for process in process_list:
        process.join()

    for fptr in output_files:
        fptr.flush()
        fsync(fptr.fileno())
        
        output_files[key].close()
    
    print('Speed Time: ', datetime.now() - start)

    gc.collect()