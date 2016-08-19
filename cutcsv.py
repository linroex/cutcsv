# Copyright 2016 HSICHELIN of copyright owner

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from os import path, fsync
from sys import argv
from datetime import datetime

import gc

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
                    output_files[key].write(''.join(output_buffer[key]))

                del output_buffer
                del keys

                gc.collect()
                
                output_buffer = {}
                keys = []
                
            current_page_size += 1
    
    for key in keys:
        output_files[key].write(''.join(output_buffer[key]))

        output_files[key].flush()
        fsync(output_files[key].fileno())
        
        output_files[key].close()

if __name__ == '__main__':
    
    base_path = path.dirname(path.realpath(__file__))

    main()