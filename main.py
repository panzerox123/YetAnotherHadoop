import json
import sys
import os
import multiprocessing
import namenode
import threading
import time
import secondarynn

config = None
NNReady = False
SNNReady = False
Exit = 0

def create_dfs():
    config_path = input("Enter path for configuration file: ")
    try:
        config_file = open(os.path.expandvars(config_path))
        config = json.load(config_file)
        config_file.close()
    except FileNotFoundError:
        print("Path does not exist")
        exit(-1)
    except json.decoder.JSONDecodeError:
        print("Configuration file not formatted correctly")
        exit(-1)
    try:
        os.makedirs(os.path.expandvars(config['path_to_datanodes']), exist_ok=True)
        os.makedirs(os.path.expandvars(config['path_to_namenodes']), exist_ok=True)
        os.makedirs(os.path.expandvars(config['datanode_log_path']), exist_ok=True)
        os.makedirs(os.path.expandvars(os.path.dirname(config['namenode_log_path'])), exist_ok=True)
        os.makedirs(os.path.expandvars(config['namenode_checkpoints']), exist_ok=True)
        os.makedirs(os.path.expandvars(config['fs_path']), exist_ok=True)
        os.makedirs(os.path.expandvars(os.path.dirname(config['dfs_setup_config'])), exist_ok=True)
        setup_config_path = os.path.expandvars(config['dfs_setup_config'])
        setup_config_file = open(setup_config_path , 'w')
        json.dump(config, setup_config_file)
        setup_config_file.close()
        cache_file = open('./cache_file', 'w')
        print(setup_config_path, file=cache_file)
        cache_file.close()
    except Exception as e:
        print("Error encountered in configuration file:", e)
        exit(-1)
    exit(0)

def master():
    
    MQueue = multiprocessing.SimpleQueue()
    MLock = multiprocessing.Lock()

    NNQueue = multiprocessing.SimpleQueue()
    NNLock = multiprocessing.Lock()
    
    SNNQueue = multiprocessing.SimpleQueue()
    SNNLock = multiprocessing.Lock()

    NN = multiprocessing.Process(target=namenode.master, args=(MQueue, MLock, NNQueue, NNLock, SNNQueue, SNNLock))
    NN.start()

    sendMsg(NNQueue, NNLock, [100, None])
    sendMsg(NNQueue, NNLock, [101, dfs_setup_config])

    SNN = multiprocessing.Process(target=secondarynn.master, args=(MQueue, MLock, NNQueue, NNLock, SNNQueue, SNNLock))
    SNN.start()

    sendMsg(SNNQueue, SNNLock, [200, None])
    sendMsg(SNNQueue, SNNLock, [201, dfs_setup_config])

    MReceiver = threading.Thread(target = Receiver, args = (NNQueue, NNLock, SNNQueue, SNNLock, MQueue, MLock))
    MReceiver.start()

    while(NNReady == False and SNNReady == False):
        continue
    
    cli(NNQueue, NNLock, SNNQueue, SNNLock)
    
    MReceiver.join()
    NN.join()
    SNN.join()

def Receiver(NNQueue, NNLock, SNNQueue, SNNLock, MQueue, MLock):
    
    while(Exit != 1):
        receiveMsg(MQueue, MLock)

def sendMsg(Queue, Lock, Data):
    
    print("Main sent", Data[0])
    Lock.acquire(block = True)
    Queue.put(Data)
    Lock.release()

def receiveMsg(Queue, Lock):
    
    Lock.acquire(block = True)

    Message = [1, None]
    
    if(Queue.empty() != True):
        
        Message = Queue.get()
        print("Main rcvd", Message[0])
    
    Lock.release()

    if(Message[0] == 0):
        return 0
    if(Message[0] == 100):
        print("Primary namenode started")
        return 100
    elif(Message[0] == 102):
        print("Primary namenode ready to Sync with Secondary namenode")
        global NNReady
        NNReady = True
        return 102
    elif(Message[0] == 103):
        return 103
    elif(Message[0] == 200):
        print("Secondary namenode started")
        return 200
    elif(Message[0] == 202):
        print("Secondary namenode ready to Sync with Primary namenode")
        global SNNReady
        SNNReady = True
        return 202
    else:
        return 1

def stopAllNodes(NNQueue, NNLock, SNNQueue, SNNLock):
    
    sendMsg(NNQueue, NNLock, [0, None])
    print("Stopping namenodes")
    sendMsg(SNNQueue, SNNLock, [0, None])
    print("Stopping Secondary namenode")
    

def cli(NNQueue, NNLock, SNNQueue, SNNLock):

    while True:
        cmd = input(">")
        if cmd.strip().lower() == 'exit':
            global Exit
            Exit = 1
            stopAllNodes(NNQueue, NNLock, SNNQueue, SNNLock)
            break
    
if __name__ == "__main__":
    try:
        dfs_setup_config_path = os.path.expandvars(sys.argv[1])
        dfs_setup_config_file = open(dfs_setup_config_path, 'r')
        dfs_setup_config = json.load(dfs_setup_config_file)
        dfs_setup_config_file.close()
        cache_file = open('./cache_file', 'w')
        print(sys.argv[1], file=cache_file)
    except IndexError:
        try:
            cache_file = open('./cache_file', 'r')
            dfs_setup_config_path = os.path.expandvars(cache_file.readline().strip())
            dfs_setup_config_file = open(dfs_setup_config_path, 'r')
            dfs_setup_config = json.load(dfs_setup_config_file)
            dfs_setup_config_file.close()
            cache_file.close()
        except:
            r = input('Would you like to create a new DFS? [y/n]: ')
            if r.lower() == 'y':
                create_dfs()
            else:
                exit(0)
    except FileNotFoundError:
        r = input('The specified DFS does not exist. Would you like to create a new one? [y/n]: ')
        if r.lower() == 'y':
            create_dfs()
        else:
            exit(0)
    except Exception as e:
        print("Unhandled exception occurred:", e)
        exit(-1)
    print("<---DFS DETAILS--->")
    for i in dfs_setup_config:
        print(i, dfs_setup_config[i], sep=':')
    print("<---DFS COMMAND LINE--->")
    master()
    exit(0)