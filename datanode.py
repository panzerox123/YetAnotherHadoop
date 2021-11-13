from io import SEEK_SET, FileIO
import os

def writer(datanode, block, data):
    with open(os.path.join(datanode, block), 'w') as f:
                f.write(data)
