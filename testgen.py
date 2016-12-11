import os
import os.path
import random

TEST_DIRECTORY = 'test'
NUM_SLAVE = 3

NUM_INPUT_FILES = 10
ENTRY_PER_FILE = 10000

ASCII_GEN = True

ascii_set = list(map(chr, range(33, 127)))

def ascii_sample(length):
    return ''.join(random.sample(ascii_set, length)).encode()

for dir_index in range(NUM_SLAVE):
    directory_name = os.path.join(TEST_DIRECTORY, 'slave%d_input' % dir_index)
    for file_index in range(NUM_INPUT_FILES):
        file_name = os.path.join(directory_name, 'input.%d' % file_index)

        file = open(file_name, 'wb')
        for entry_index in range(ENTRY_PER_FILE):
            if ASCII_GEN:
                file.write(ascii_sample(10))
                file.write(b' ')
                file.write(ascii_sample(88))
                file.write(b'\n')
            else:
                file.write(os.urandom(100))
        file.close()
