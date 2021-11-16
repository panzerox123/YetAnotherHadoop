import os
import fileinput
import socket
import tqdm

'''def updateLog(log, datanode, block, action):
    log = log + "/datanode"+datanode[-1]
    with open(log, "r") as f:
        contents = f.readlines()
        f.close()
    if(len(contents)+1 < block):
        with open(log, "a") as f:
            f.write(str(block)+", "+str(action))
    else:
        with open(log, "w") as f:
            for line in fileinput.FileInput(log,inplace=1):
                if line[0] == block:
                    line=line.replace(line,str(block)+", "+str(action))


def writer(datanode, block, data, log):
    with open(os.path.join(datanode, "block"+block), 'w') as f:
                f.write(data)
                f.close()
    updateLog(log, datanode, block, 1)


def createNodes(path, num):
    for i in range(num):
        os.makedirs(os.path.expandvars(path+"/datanode"+str(i+1)))
'''

SERVER_HOST = "0.0.0.0"

BUFFER_SIZE = 4096
SEPARATOR = "<SEPARATOR>"


class Datanode:
    def __init__(self, path, size, blocks, logPath, num, port):
        
        self.path = path
        self.size = size
        self.blocks = blocks
        self.logPath = logPath
        self.num = num
        self.SERVER_PORT = port
        s = socket.socket()

        s.bind((SERVER_HOST, self.SERVER_PORT))
        print(f"[*] Listening as {SERVER_HOST}:{self.SERVER_PORT}")
        
        client_socket, address = s.accept() 
        self.server(client_socket)
        pass

    # def recieve files
    # def write files
    # def update datanode log
    # def return status to namenode

    def server(client_socket):
        
        received = client_socket.recv(BUFFER_SIZE).decode()
        filename, filesize = received.split(SEPARATOR)
        # remove absolute path if there is
        filename = os.path.basename(filename)
        # convert to integer
        filesize = int(filesize)
        #filename can be the block number specified by the namenode since its keeping track anyway
        progress = tqdm.tqdm(range(filesize), f"Receiving {filename}", unit="B", unit_scale=True, unit_divisor=1024)
        while True:
            with open(filename, "wb") as f:
                while True:
                    bytes_read = client_socket.recv(BUFFER_SIZE)
                    if not bytes_read:   
                        break
                    f.write(bytes_read)
                    progress.update(len(bytes_read))

            # close the client socket
        client_socket.close()
        # close the server socket
        s.close()
        pass