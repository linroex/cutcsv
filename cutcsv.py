from os import path
from sys import argv
from datetime import datetime

# import timeit
import cProfile

def main():
    target_file_path = argv[1]
    decision_column_index = int(argv[2])

    with open(target_file_path, 'r', encoding='utf8') as f:
        keys = []
        output_buffer = {}

        for line in f:
            key = line.split(',')[decision_column_index]
            if key in keys:
                output_buffer[key].append(line)
            else:
                keys.append(key)
                output_buffer[key] = [line]

    for key in keys:
        with open(base_path + '/' + key + '.csv', 'w', encoding='utf8') as f:
            f.write('\n'.join(output_buffer[key]))

if __name__ == '__main__':

    base_path = path.dirname(path.realpath(__file__))

    # start = datetime.now()
    # main()
    # print(datetime.now() - start)

    # print(timeit.timeit(main, number=6))
    cProfile.run('main()')
