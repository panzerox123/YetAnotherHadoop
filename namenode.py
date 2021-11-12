import math
import sys
import time
MB=1000000

def convert_to_blocks(file_name,block_size):
    f=open(file_name,'rb')
    tot_bytes=0
    for l in f:
        tot_bytes+=sys.getsizeof(l)
    tot_mb=tot_bytes/MB
    tot_splits=math.ceil(tot_mb/block_size)
    return tot_splits

def sendMsg(Queue, Lock, Data):
    
    Lock.acquire(block = True)
    Queue.put(Data)
    Lock.release()

def receiveMsg(Queue, Lock):
    
    Lock.acquire(block = True)

    Message = 1

    while(Queue.empty() != True):
        Message = Queue.get()
        
        if(Message == 0):
            break
        else:
            print(Message)

    Lock.release()

    if(Message == 0):
        return 0
    else:
        return 1

def master(MainQueue, MainLock):

    Temp = 1
    while(Temp):
        Temp = receiveMsg(MainQueue, MainLock)
        time.sleep(1)
