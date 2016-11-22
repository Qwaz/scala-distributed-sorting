import os
import os.path

TEST_DIRECTORY = 'test'
NUM_SLAVE = 3

NUM_INPUT_FILES = 10
ENTRY_PER_FILE = 100

for dir_index in range(NUM_SLAVE):
    directory_name = os.path.join(TEST_DIRECTORY, 'slave%d_input' % dir_index)
    for file_index in range(NUM_INPUT_FILES):
        file_name = os.path.join(directory_name, 'input.%d' % file_index)

        file = open(file_name, 'wb')
        for entry_index in range(ENTRY_PER_FILE):
            file.write(os.urandom(100))
        file.close()
