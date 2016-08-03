from os import path
from sys import argv
from datetime import datetime

import gc

# import timeit
# import cProfile

def main():
    target_file_path = argv[1]
    decision_column_index = int(argv[2])

    output_files = {}

    with open(target_file_path, 'r', encoding='utf8') as f:
        keys = []
        output_buffer = {}

        max_page_size = 1000

        for line in f:
            key = line.split(',')[decision_column_index]
            if key in keys:
                output_buffer[key].append(line)
            else:
                keys.append(key)
                output_files[key] = open(base_path + '/' + key + '.csv', 'w', encoding='utf8')
                output_buffer[key] = [line]

            if len(output_buffer[key]) >= max_page_size:
                output_files[key].write('\n'.join(output_buffer[key]))
                output_buffer[key] = []
                gc.collect()

    for key in keys:
        output_files[key].close()

if __name__ == '__main__':
    
    gc.enable()

    base_path = path.dirname(path.realpath(__file__))

    start = datetime.now()
    main()
    print(datetime.now() - start)

    gc.collect()

    # print(timeit.timeit(main, number=6))
    # cProfile.run('main()')
