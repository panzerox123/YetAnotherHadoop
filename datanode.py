import json
import os
import fileinput
import socket
import tqdm

SERVER_HOST = "0.0.0.0"
BUFFER_SIZE = 2
SEPARATOR = "<SEPARATOR>"


class Datanode:
    def __init__(self, config, path, port):
        self.config = config
        self.datanode_path = path
        self.SERVER_PORT = port
        self.datanode_socket = socket.socket()
        self.datanode_socket.bind(('', self.SERVER_PORT))
        self.datanode_socket.listen(5)
        self.datanodeRunningLoop = True
        print(f"[*] Listening as {SERVER_HOST}:{self.SERVER_PORT}")

    # def recieve files
    # def write files
    # def update datanode log
    # def return status to namenode

    def reciever(self):
        while self.datanodeRunningLoop:
            namenode_receiver_socket, namenode_reciever_addr = self.datanode_socket.accept()
            namenode_receiver_socket.setblocking(False)
            print('accepted:', namenode_reciever_addr)
            buf = []
            while True:
                try:
                    bytes_read = namenode_receiver_socket.recv(BUFFER_SIZE)
                except:
                    break
                if not bytes_read:
                    break
                buf.append(bytes_read)
            data = b''.join(buf)
            data = json.loads(data.decode())
            if data['code'] == 0:
                print(data)
                self.datanode_socket.close()
                self.datanodeRunningLoop = False
            namenode_receiver_socket.close()


    def writer(self):
        self.namenode_receiver_socket, self.namenode_receiver_addr = self.datanode_socket.accept()
        received = self.namenode_receiver_socket.recv(BUFFER_SIZE).decode()
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
                    bytes_read = self.namenode_receiver_socket.recv(BUFFER_SIZE)
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
    dn.reciever()
    exit(0)
