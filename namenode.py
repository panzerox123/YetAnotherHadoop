import math
import sys
import time
MB = 1048576

def convert_to_blocks(file_name,block_size):
    f=open(file_name,'rb')
    tot_bytes=0
    for l in f:
        tot_bytes+=sys.getsizeof(l)
    tot_mb=tot_bytes/MB
    tot_splits=math.ceil(tot_mb/block_size)
    return tot_splits

class PrimaryNameNode():
    def __init__(self,queue, lock):
        self.queue = queue
        self.lock = lock
    
    def sendMsg(self, Queue, Data):
        self.lock.acquire(block = True)
        self.queue.put(Data)
        self.lock.release()

    def checkQueue(self):
        self.lock.acquire(block = True)
        Message = 1
        while(self.queue.empty() != True):
            Message = self.queue.get()
            if(Message == 0):
                break
            else:
                print(Message)
        self.lock.release()
        if(Message == 0):
            return 0
        else:
            return 1

def nn_loop(mainQueue, mainLock):
    status = 1
    nn = PrimaryNameNode(mainQueue, mainLock)
    while(status):
        status = nn.checkQueue()
        time.sleep(1)
