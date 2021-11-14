import math
import sys
import time
import json
from fsplit.filesplit import Filesplit
import threading
import json
import os
import shutil
fs = Filesplit()

MB=1000000

def get_tot_split(file_name,block_size): #contains the file split function
    f=open(file_name,'rb')
    tot_bytes=0
    for l in f:
        tot_bytes+=sys.getsizeof(l)
    tot_mb=tot_bytes/MB
    tot_splits=math.ceil(tot_mb/block_size)
    # fs.split(file='trial\Trial.pdf',split_size=tot_bytes//tot_splits,output_dir='out')
    # fs.merge(input_dir='out')
    return tot_splits

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
        try:
            namenode_json_file = open(self.namenode_json_path, 'r')
            self.namenode_config = json.load(namenode_json_file)
            namenode_json_file.close()
        except:
            self.sendMsg(mQueue, mLock, [101, None])
            self.namenode_config = None
        self.SNNSyncThread = threading.Thread(target = self.SNNSync)
        self.SNNSyncThread.start()

    def format_namenode(self):
        dn_paths = []
        dn_remaining = []
        dn_status = []
        for i in range(self.config["num_datanodes"]):
            dn_path = os.path.join(self.config['path_to_datanodes'],str(i))
            dn_status.append('idle')
            dn_remaining.append(self.config['datanode_size'])
            dn_paths.append(dn_path)
        for i in dn_paths:
            shutil.rmtree(i, ignore_errors=True)
        for i in dn_paths:
            os.mkdir(i)
        self.namenode_config = {
            "block_size": self.config["block_size"],
            "datanode_size": self.config["datanode_size"],
            "num_datanodes": self.config["num_datanodes"],
            "datanode_paths": dn_paths,
            "datanode_status" : dn_status,
            "datanode_remaining" : dn_remaining,
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

    def mkdir(self, path):
        pass

    def mkdir_parent(self, path):
        pass

    def rmdir(self, path):
        pass

    def sendMsg(self, queue, lock, data):
        print("namenode sent", data[0])
        lock.acquire(block = True)
        queue.put(data)
        lock.release()

    def receiveMsg(self, queue, lock):
        lock.acquire(block = True)
        if(not queue.empty()):
            message = queue.get()
            print("secondary namenode rcvd", message[0])
        else:
            message = [1, None]
        lock.release()
        if(message[0] == 0):
            self.pnnLoopRunning = False
            self.SNNSyncThread.join()
            return 0
        elif(message[0] == 101):
            self.format_namenode()
            return 101
        elif(message[0] == 100):
            self.sendMsg(self.mQueue, self.mLock, [100, None])
            return 100
        else:
            return 1

    def SNNSync(self):
        self.sendMsg(self.mQueue, self.mLock, [102, None])
        timeout=time.time()+self.config['sync_period']
        while True:
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
        while True:
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
