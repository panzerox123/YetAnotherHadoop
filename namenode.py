import math
import sys
import time
import threading

MB=1000000

def convert_to_blocks(file_name,block_size):
    f=open(file_name,'rb')
    tot_bytes=0
    for l in f:
        tot_bytes+=sys.getsizeof(l)
    tot_mb=tot_bytes/MB
    tot_splits=math.ceil(tot_mb/block_size)
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
        self.SNNSyncThread = threading.Thread(target = self.SNNSync)
        self.SNNSyncThread.start()

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
        elif(message[0] == 100):
            self.sendMsg(self.mQueue, self.mLock, [100, None])
            return 100
        else:
            return 1

    def SNNSync(self):
        self.sendMsg(self.mQueue, self.mLock, [102, None])
        while(self.pnnLoopRunning):
            self.sendMsg(self.snnQueue, self.snnLock, [103, None])
            time.sleep(self.config["sync_period"])
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
        self.heartbeat = 1
        while(self.snnLoopRunning):
            if(self.heartbeat == 1):
                self.heartbeat = 0
                print("heartbeat received")
            else:
                time.sleep(self.config["sync_period"] * 0.5)
                if(self.heartbeat == 1):
                    self.heartbeat = 0
                    print("heartbeat received")
                else:
                    break
            time.sleep(self.config["sync_period"])
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
