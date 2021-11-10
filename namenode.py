import time


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