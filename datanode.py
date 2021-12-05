import json
import os
import fileinput
import socket
import tqdm

SERVER_HOST = "0.0.0.0"
BUFFER_SIZE = 4096
SEPARATOR = "<SEPARATOR>"
MB=1048576


class Datanode:
    def __init__(self, config, path, port):
        self.config = config
        self.datanode_path = path
        self.SERVER_PORT = port
        self.datanode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
            # namenode_receiver_socket.setblocking(False)
            namenode_receiver_socket.settimeout(2)
            print('accepted:', namenode_reciever_addr)
            buf = []
            while True:
                try:
                    bytes_read = namenode_receiver_socket.recv(self.config['block_size']*MB)
                except:
                    break
                if not bytes_read:
                    break
                buf.append(bytes_read)
            if(len(buf) == 0):
                print('resend packet')
                continue
            data = b''.join(buf)
            try:
                data = json.loads(data.decode())
            except Exception as e:
                print('Err: ', buf, e)
                
            if data['code'] == 0:
                # print(data)
                self.datanode_socket.close()
                self.datanodeRunningLoop = False
            elif data['code'] == 301:
                write_path = os.path.join(self.datanode_path, data['file_name'])
                write_file = open(write_path, 'wb')
                write_file.write(data['packet_data'].encode())
                write_file.close()
                namenode_receiver_socket.sendall(json.dumps({'code': 3010}).encode())
            elif data['code'] == 302:
                packet = dict()
                try:
                    read_path = os.path.join(self.datanode_path, data['file_name'])
                    read_file = open(read_path, 'rb')
                    packet_data = read_file.read(self.config['block_size']*MB)
                    packet['code'] = 3020
                    packet['packet_data'] = packet_data.decode()
                except:
                    packet['code'] = 3021
                namenode_receiver_socket.sendall(json.dumps(packet).encode()) 
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
                    bytes_read = self.namenode_receiver_socket.recv(self.config['block_size'])
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
