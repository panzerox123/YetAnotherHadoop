import math
import sys
import time
import json
from typing import Pattern
from fsplit.filesplit import Filesplit
import threading
import json
import os
import socket
import tqdm
import shutil
import multiprocessing
import datanode

MB=1048576 
BUFFER_SIZE = 4096

def get_tot_split(file_name,block_size): #contains the file split function
    f=open(file_name,'rb')
    tot_bytes=0
    for l in f:
        tot_bytes+=sys.getsizeof(l)
    tot_mb=tot_bytes/MB
    tot_splits=math.ceil(tot_mb/block_size)
    return tot_bytes, tot_splits

class PrimaryNameNode:
    def __init__(self, mQueue, mLock, pnnQueue, pnnLock, snnQueue, snnLock, config):
        self.pnnLoopRunning = True
        self.mQueue = mQueue
        self.mLock = mLock
        self.pnnQueue = pnnQueue
        self.pnnLock = pnnLock
        self.snnQueue = snnQueue
        self.snnLock = snnLock
        self.config = config
        self.namenode_json_path = os.path.join(self.config["path_to_namenodes"], "namenode.json")
        # self.free_ptr = 0
        try:
            namenode_json_file = open(self.namenode_json_path, 'r')
            self.namenode_config = json.load(namenode_json_file)
            namenode_json_file.close()
        except:
            self.sendMsg(mQueue, mLock, [101, None])
            self.namenode_config = None
        
        self.dnIndex = {
            "tot_emp": self.config["num_datanodes"]*self.config["datanode_size"],
            "rep_factor": int(self.config["replication_factor"]),
            "blacklist": [0]*self.config["num_datanodes"]
        }
        for i in range(self.config["num_datanodes"]):
            self.dnIndex["dn"+str(i+1)] = [0]*self.config["datanode_size"]

        self.SNNSyncThread = threading.Thread(target = self.SNNSync)
        self.SNNSyncThread.start()
        self.initialise_datanodes()
    
    def initialise_datanodes(self):
        self.datanode_process_list = []
        self.datanode_port_list = []
        init_port = 9000
        for i in range(self.config['num_datanodes']):
            self.datanode_port_list.append(i+init_port)
            self.datanode_process_list.append(threading.Thread(target=datanode.datanode_thread, args=(self.config, self.namenode_config['datanode_paths'][i], init_port+i)))
        for i in self.datanode_process_list:
            i.start()

    def DNMsg(self, datanode_num, data):
        self.namenode_socket = socket.socket()
        # self.namenode_socket.setblocking(0)
        # self.namenode_socket.settimeout(1)
        self.namenode_socket.connect(('', self.datanode_port_list[datanode_num]))
        self.namenode_socket.send(json.dumps(data).encode())
        buf = []
        while True:
            try:
                bytes_read = self.namenode_socket.recv(BUFFER_SIZE)
            except:
                break
            if not bytes_read:
                break
            buf.append(bytes_read)
        if len(buf) > 0:
            output = b''.join(buf)
            output = json.loads(output.decode())
            print(output)
        self.namenode_socket.close()


    def format_namenode(self):
        dn_paths = []
        dn_remaining = []
        dn_status = []
        for i in range(self.config["num_datanodes"]):
            dn_path = os.path.join(self.config['path_to_datanodes'],str(i))
            dn_remaining.append(self.config['datanode_size'])
            dn_paths.append(dn_path)
        for i in dn_paths:
            shutil.rmtree(i, ignore_errors=True)
        for i in dn_paths:
            os.mkdir(i)
        free_mat = []
        for i in range(self.config['datanode_size']):
            for j in range(self.config['num_datanodes']):
                free_mat.append((j, True))
        self.namenode_config = {
            "block_size": self.config["block_size"],
            "datanode_size": self.config["datanode_size"],
            "num_datanodes": self.config["num_datanodes"],
            "datanode_paths": dn_paths,
            "datanode_remaining" : dn_remaining,
            "free_matrix" : free_mat,
            "fs_root": {
                "type" : "dir",
                "data" : {}
            }
        }
        namenode_json_file = open(self.namenode_json_path, 'w')
        json.dump(self.namenode_config, namenode_json_file)
        namenode_json_file.close()

    def dumpNameNode(self):
        namenode_json_file = open(self.namenode_json_path, 'w')
        json.dump(self.namenode_config, namenode_json_file)
        namenode_json_file.close()

    def mkdir_recur(self, folder_arr, curr, parent=True) -> dict:
        if not parent:
            if len(folder_arr) > 1 and folder_arr[0] not in curr.keys():
                raise FileNotFoundError
        if len(folder_arr) > 0:
            if folder_arr[0] not in curr.keys():
                curr[folder_arr[0]] = {
                    "type" : 'dir',
                    "data" : {}
                }
            curr[folder_arr[0]]['data'] = self.mkdir_recur(folder_arr[1:], curr[folder_arr[0]]['data'], parent)
            return curr
        else:
            return {}


    def mkdir(self, path):
        folders = path.split('/')
        try:
            self.namenode_config['fs_root']['data'] = self.mkdir_recur(folders[1:], self.namenode_config['fs_root']['data'], False)
        except FileNotFoundError:
            self.sendMsg(self.mQueue, self.mLock, [1041, None])
            return
        self.dumpNameNode()
        self.sendMsg(self.mQueue, self.mLock, [1040, None])

    def mkdir_parent(self, path):
        folders = path.split('/')
        try:
            self.namenode_config['fs_root']['data'] = self.mkdir_recur(folders[1:], self.namenode_config['fs_root']['data'], True)
        except FileNotFoundError:
            self.sendMsg(self.mQueue, self.mLock, [1051, None])
            return
        self.dumpNameNode()
        self.sendMsg(self.mQueue, self.mLock, [1050, None])

    def rmdir_recur(self, folder_arr, curr):
        if(len(folder_arr) > 1):
            if(folder_arr[0] not in curr.keys()):
                raise FileNotFoundError
            else:
                self.rmdir_recur(folder_arr[1:], curr[folder_arr[0]]['data'])
        elif len(folder_arr) == 1:
            if(folder_arr[0] not in curr.keys()):
                raise FileNotFoundError
            else:
                if(curr[folder_arr[0]]['data'] == {}):
                    del curr[folder_arr[0]]

    def rmdir(self, path):
        folders = path.split('/')
        try:
            self.rmdir_recur(folders[1:], self.namenode_config['fs_root']['data'])
        except FileNotFoundError:
            self.sendMsg(self.mQueue, self.mLock, [1061, None])
            return
        self.sendMsg(self.mQueue, self.mLock, [1060, None])
        self.dumpNameNode()

    def sendMsg(self, queue, lock, data):
        print("namenode sent", data[0])
        lock.acquire(block = True)
        queue.put(data)
        lock.release()
    
    def ls_recur(self, curr, path):
        print(path)
        for i in curr['data']:
            if curr['data'][i]['type'] == 'file':
                print(path+i)
            elif curr['data'][i]['type'] == 'dir':
                self.ls_recur(curr['data'][i], path+i+'/')
        
    def ls(self):
        self.ls_recur(self.namenode_config['fs_root'], '/')
    
    def write(self, block, file, dn_num, dir):
        pass

    def free_space(self) -> int:
        free = 0
        for i in self.namenode_config['datanode_remaining']:
            free+=i
        return free

    def return_free_ptr(self) -> int:
        free_ptr = 0
        while not self.namenode_config['free_matrix'][free_ptr][1] and free_ptr < self.namenode_config['num_datanodes'] * self.namenode_config['datanode_size']:
            free_ptr += 1
        return free_ptr

    def put_recur(self, path_arr, curr, file_name, file_data):
        if(len(path_arr) > 1):
            if path_arr[0] not in curr.keys():
                raise FileNotFoundError
            else:
                curr[path_arr[0]]['data'] = self.put_recur(path_arr[1:], curr[path_arr[0]]['data'], file_name, file_data)
                return curr
        else:
            curr[path_arr[0]]['data'][file_name] = file_data
            return curr

        
    def put(self, file_path, hdfs_path):
        file_name = os.path.basename(file_path)
        path_arr = hdfs_path.split('/')
        file_data = {
            'type': 'file',
        }
        blocks = {}
        total_size, total_splits = get_tot_split(file_path, self.config['block_size'])
        if(3*total_splits > self.free_space()):
            self.sendMsg(self.mQueue, self.mLock, [1081, None])
            return
        for i in range(total_splits):
            blks = []
            for j in range(self.config['replication_factor']):
                write_to_node = self.return_free_ptr()
                self.namenode_config['free_matrix'][write_to_node][1] = False
                self.namenode_config['datanode_remaining'][self.namenode_config['free_matrix'][write_to_node][0]] -= 1
                blks.append(self.namenode_config['free_matrix'][write_to_node][0])
            blocks[i] = blks
        file_data['blocks'] = blocks
        try:
            self.namenode_config['fs_root']['data'] = self.put_recur(path_arr[1:], self.namenode_config['fs_root']['data'],file_name, file_data) 
        except Exception as e:
            print("Error", e)
            self.sendMsg(self.mQueue, self.mLock, [1081, None])
            return
        self.dumpNameNode()
        self.sendMsg(self.mQueue, self.mLock, [1080, None])
        '''
        tot_bytes,tot = get_tot_split(file_path, self.namenode_config["block_size"])
        if((tot*self.config["replication_factor"])>self.dnIndex["tot_emp"]):
            print("Not enough space :D")
        else:
            # going to write some stuff here
            out = "/Users/utkarshgupta/Documents/DevWork/YetAnotherHadoop/out"
            os.makedirs(os.path.expandvars(out), exist_ok=True)
            fs.split(file=file_path,split_size=tot_bytes//tot,output_dir='out')
            dn_num = 0
            for i in range(tot):
                for j in range(self.config["replication_factor"]):
                    while(not self.dnIndex["blacklist"][dn_num]):
                        dn_num = (dn_num + 1)%len(self.dnIndex["blacklist"])
                    self.write(i, file_path, dn_num, out)
        '''


    def receiveMsg(self, queue, lock):
        lock.acquire(block = True)
        if(not queue.empty()):
            message = queue.get()
            print("namenode rcvd", message[0])
        else:
            message = [1, None]
        lock.release()
        if(message[0] == 0):
            self.pnnLoopRunning = False
            self.SNNSyncThread.join()
            for i in range(self.namenode_config['num_datanodes']):
                self.DNMsg(i, {'code': 0})
            for i in self.datanode_process_list:
                i.join()
            # self.DNMsg(0, {'code': 0})
            return 0
        elif(message[0] == 101):
            self.format_namenode()
            return 101
        elif(message[0] == 100):
            self.sendMsg(self.mQueue, self.mLock, [100, None])
            return 100
        elif(message[0] == 104):
            self.mkdir(message[1])
            return 104
        elif(message[0] == 105):
            self.mkdir_parent(message[1])
            return 105
        elif(message[0] == 106):
            self.rmdir(message[1])
            return 106
        elif(message[0] == 107):
            self.ls()
            return 107
        elif(message[0] == 108):
            self.put(message[1], message[2])
            return 108
        else:
            return 1

    def SNNSync(self):
        self.sendMsg(self.mQueue, self.mLock, [102, None])
        timeout=time.time()+self.config['sync_period']
        while self.pnnLoopRunning:
            if(time.time()>timeout):
                self.sendMsg(self.snnQueue, self.snnLock, [103, None])
                timeout=time.time()+self.config['sync_period']
        exit(0)

class SecondaryNameNode():
    def __init__(self, mQueue, mLock, pnnQueue, pnnLock, snnQueue, snnLock, config):
        self.snnLoopRunning = True
        self.heartbeat = True
        self.name_node_crash = False
        self.mQueue = mQueue
        self.mLock = mLock
        self.pnnQueue = pnnQueue
        self.pnnLock = pnnLock
        self.snnQueue = snnQueue
        self.snnLock = snnLock
        self.config = config
        self.PNNSyncThread = threading.Thread(target = self.PNNSync)
        self.PNNSyncThread.start()

    def sendMsg(self, queue, lock, data):
        print("secondary namenode sent", data[0])
        lock.acquire(block = True)
        queue.put(data)
        lock.release()

    def receiveMsg(self, queue, lock):
        lock.acquire(block = True)
        if(not queue.empty()):
            message = queue.get()
            print("Secondary namenode rcvd", message[0])
        else:
            message = [1, None]
        lock.release()
        if(message[0] == 0):
            self.snnLoopRunning = False
            self.PNNSyncThread.join()
            return 0
        elif(message[0] == 200):
            self.sendMsg(self.mQueue, self.mLock, [200, None])
            return 200
        elif(message[0] == 203):
            self.heartbeat = 1
            return 203
        else:
            return 1
    
    def pnnCrashStatus(self):
        if(self.name_node_crash):
            self.sendMsg(self.mQueue, self.mLock, [204, None])
            return True
        else:
            return False

    def PNNSync(self):
        self.sendMsg(self.mQueue, self.mLock, [202, None])
        timeout=time.time()+self.config['sync_period']
        while self.snnLoopRunning:
            if(time.time()>timeout):
                tt=time.time()+self.config['sync_period']*0.5
                if(self.heartbeat==1):
                    self.heartbeat = 0
                    print("heartbeat received")
                    self.snnLoopRunning=False
                    break
                while True:
                    if(time.time()>tt):
                        if(self.heartbeat == 1):
                            self.heartbeat = 0
                            self.snnLoopRunning=False
                            print("heartbeat received")
                        else:
                            break
                if(self.snnLoopRunning):
                    break
        if(self.snnLoopRunning):
            self.name_node_crash = True
        exit(0)

def secondary_namenode_thread(mQueue, mLock, pnnQueue, pnnLock, snnQueue, snnLock, config):
    snn = SecondaryNameNode(mQueue, mLock, pnnQueue, pnnLock, snnQueue, snnLock, config)
    status = 1
    while(status):
        status = snn.receiveMsg(snnQueue, snnLock)
        if(snn.pnnCrashStatus()):
            primary_namenode_thread(mQueue, mLock, pnnQueue, pnnLock, snnQueue, snnLock, config)
    exit(0)


def primary_namenode_thread(mQueue, mLock, pnnQueue, pnnLock, snnQueue, snnLock, config):
    pnn = PrimaryNameNode(mQueue, mLock, pnnQueue, pnnLock, snnQueue, snnLock, config)
    status = 1
    while(status):
        status = pnn.receiveMsg(pnnQueue, pnnLock)
    exit(0)
