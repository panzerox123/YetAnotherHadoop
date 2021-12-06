import json
import os
import fileinput
import socket
import tqdm
import time

SERVER_HOST = "0.0.0.0"
BUFFER_SIZE = 4096
SEPARATOR = "<SEPARATOR>"
MB=1048576


class Datanode:
    def __init__(self, config, path, port, i):
        self.config = config
        self.datanode_path = path
        self.SERVER_PORT = port
        self.datanode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.datanode_socket.bind(('', self.SERVER_PORT))
        self.datanode_socket.listen(5)
        self.datanodeRunningLoop = True
        self.log = open(self.config["datanode_log_path"] + str(i) + ".txt", "w")
        self.log.write("[*] Listening as :" + str(self.SERVER_PORT))

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
                continue
            data = b''.join(buf)
            try:
                data = json.loads(data.decode())
                self.log.write("Data -" + str(data) + "received at" + str(time.time()))
            except Exception as e:
                self.log.write("Error -" + str(e) + "while receiving data at" + str(time.time()))
                
            if data['code'] == 0:
                # print(data)
                self.datanode_socket.close()
                self.datanodeRunningLoop = False
                self.log.write("Shutting down datanode at" + str(time.time()))
                self.log.close()
            elif data['code'] == 301:
                write_path = os.path.join(self.datanode_path, data['file_name'])
                write_file = open(write_path, 'wb')
                write_file.write(data['packet_data'].encode())
                write_file.close()
                self.log.write("Sending data -" + str(json.dumps({'code': 3010}).encode()) + "at" + str(time.time()))
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
                self.log.write("Sending data -" + str(json.dumps(packet).encode()) + "at" + str(time.time()))
                namenode_receiver_socket.sendall(json.dumps(packet).encode())
            namenode_receiver_socket.close()

def datanode_thread(config, path, port):
    dn = Datanode(config, path, port)
    dn.reciever()
    exit(0)
