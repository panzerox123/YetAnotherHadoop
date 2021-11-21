import os
import fileinput
import socket
import tqdm

SERVER_HOST = "0.0.0.0"
BUFFER_SIZE = 4096
SEPARATOR = "<SEPARATOR>"


class Datanode:
    def __init__(self, config, path, port):
        self.config = config
        self.datanode_path = path
        self.SERVER_PORT = port
        self.datanode_socket = socket.socket()
        self.datanode_socket.bind((SERVER_HOST, self.SERVER_PORT))
        print(f"[*] Listening as {SERVER_HOST}:{self.SERVER_PORT}")

    # def recieve files
    # def write files
    # def update datanode log
    # def return status to namenode

    def reciever(self, client_socket, path):
        self.namenode_receiver_socket, self.namenode_receiver_addr = self.datanode_socket.accept()
        received = client_socket.recv(BUFFER_SIZE).decode()
        filename, filesize = received.split(SEPARATOR)
        # remove absolute path if there is
        filename = os.path.basename(filename)
        # convert to integer
        filesize = int(filesize)
        #filename can be the block number specified by the namenode since its keeping track anyway
        progress = tqdm.tqdm(range(filesize), f"Receiving {filename}", unit="B", unit_scale=True, unit_divisor=1024)
        while True:
            with open(self.path+"/"+str(filename), "wb") as f:
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


def datanode_thread(config, path, port):
    dn = Datanode(config, path, port)
