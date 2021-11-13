import math
import sys
import time
import threading

config = None

heartbeat = 0
Exit = 0
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
    
    print("secondary namenode sent", Data[0])
    Lock.acquire(block = True)
    Queue.put(Data)
    Lock.release()

def receiveMsg(Queue, Lock):
    
    if(Lock.acquire(block = True)):

        Message = [1, None]

        if(Queue.empty() != True):
            
            Message = Queue.get()
            print("Secondary namenode rcvd", Message[0])
        
        Lock.release()

        if(Message[0] == 0):
            return 0
        elif(Message[0] == 200):
            return 200
        elif(Message[0] == 201):
            global config
            config = Message[1]
            return 201
        elif(Message[0] == 203):
            heartbeat = 1
            return 203
        else:
            return 1
    else:
        return 1

def master(MQueue, MLock, NNQueue, NNLock, SNNQueue, SNNLock):
    
    Code = 1

    while(Exit != 1):
        
        Code = receiveMsg(SNNQueue, SNNLock)
        
        if(Code == 200):
            sendMsg(MQueue, MLock, [200, None])
        elif(Code == 201):
            SecondaryNNSync = threading.Thread(target = SNNSync, args = (MQueue, MLock, SNNQueue, SNNLock))
            SecondaryNNSync.start()
        elif(Code == 0):
            SecondaryNNSync.join()

def SNNSync(MQueue, MLock, SNNQueue, SNNLock):

    sendMsg(MQueue, MLock, [202, None])
    heartbeat = 1

    while(Exit != 1):
        if(heartbeat == 1):
            heartbeat = 0
            print("heartbeat received")
        else:
            time.sleep(config["sync_period"] * 0.5)
            
            if(heartbeat == 1):
                heartbeat = 0
                print("heartbeat received")
            else:
                sendMsg(MQueue, MLock, [204, None])
                break

        time.sleep(config["sync_period"])
    
    if(Exit != 0):
        masterNameNode(MQueue, MLock, SNNQueue, SNNLock)

def masterNameNode(MainQueue, MainLock):
    pass