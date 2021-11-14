import math
import sys
import time
import json
from fsplit.filesplit import Filesplit
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

#Initializing namenode json file

def init_json(fs_name):
    data={}
    data[fs_name]=[]
    with open('namenode.json','w') as f:
        json.dump(data,f)

#Infinite loop part (Don't uncomment)

# timeout=time.time()+60*3

# while True:
#     if(time.time()>timeout):
#         timeout=time.time()+60*3
#         break