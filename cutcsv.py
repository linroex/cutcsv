from os import path, fsync
from sys import argv
from datetime import datetime
from multiprocessing import Process
from math import floor, ceil

import gc

def count_offset(fpath, number):
    file_size = int(path.getsize(fpath))
    each_size = file_size / number
    offsets = []

    with open(fpath, 'r', encoding='utf8') as fptr:
        for i in range(number):
            fptr.seek(each_size * i)
            if i != 0:
                fptr.readline()
            offsets.append(fptr.tell())
    
    offsets.append(file_size)

    return offsets

def handle(current_offset, next_offset, path):
    with open(path, 'r', encoding='utf8') as fptr:
        fptr.seek(current_offset)

        max_page_size = 1000
        current_page_size = 0

        output_buffer = {}
        keys = []

        for i in range(next_offset):
            line = fptr.readline()
            try:
                key = line.split(',')[decision_column_index]
            except IndexError:
                print(fptr.tell(), next_offset)

            if (fptr.tell() > next_offset) or line == '':
                for f_key in keys:
                    output_files[f_key].write(''.join(output_buffer[f_key]))
                return True

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

            
        
if __name__ == '__main__':
    gc.enable()

    base_path = path.dirname(path.realpath(__file__))

    output_files = {}

    target_file_path = argv[1]
    decision_column_index = int(argv[2])
    
    process_number = 2
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