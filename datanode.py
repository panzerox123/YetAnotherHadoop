import json
import os
import socket
import logging

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
        self.datanode_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.datanode_socket.bind(('', self.SERVER_PORT))
        self.datanode_socket.listen(5)
        self.datanodeRunningLoop = True
        logging.basicConfig(filename=os.path.join(self.config['datanode_log_path'], 'DATANODE_'+str(port)+'.log'), format='%(asctime)s %(message)s', filemode='a')
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.info(f"Listening as {SERVER_HOST}:{self.SERVER_PORT}")

    # def recieve files
    # def write files
    # def update datanode log
    # def return status to namenode

    def reciever(self):
        while self.datanodeRunningLoop:
            namenode_receiver_socket, namenode_reciever_addr = self.datanode_socket.accept()
            # namenode_receiver_socket.setblocking(False)
            namenode_receiver_socket.settimeout(2)
            self.logger.info('Connection accepted:{}'.format(namenode_reciever_addr))
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
                self.logger.warning('Empty buffer recieved')
                continue
            data = b''.join(buf)
            try:
                data = json.loads(data.decode())
            except Exception as e:
                self.logger.error('Err: {}'.format(e))
                
            if data['code'] == 0:
                # print(data)
                self.logger.info("Exiting")
                self.datanode_socket.close()
                self.datanodeRunningLoop = False
            elif data['code'] == 301:
                self.logger.info("Writing data: {}".format(data['file_name']))
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
                    self.logger.info("Reading data: {}".format(data['file_name']))
                except:
                    packet['code'] = 3021
                namenode_receiver_socket.sendall(json.dumps(packet).encode()) 
            namenode_receiver_socket.close()

def datanode_thread(config, path, port):
    dn = Datanode(config, path, port)
    dn.reciever()
    exit(0)
