import json
import sys
import os
import multiprocessing
import namenode

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
    
    MainNamenodeQueue = multiprocessing.Queue()
    MainNamenodeLock = multiprocessing.Lock()
    Namenode = multiprocessing.Process(target=namenode.master, args=((MainNamenodeQueue), MainNamenodeLock))
    Namenode.start()

    cli(MainNamenodeQueue, MainNamenodeLock)
    
    Namenode.join()

def sendMsg(Queue, Lock, Data):
    
    Lock.acquire(block = True)
    Queue.put(Data)
    Lock.release()

def receiveMsg(Queue, Lock):
    
    Lock.acquire(block = True)

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

def stopAllNodes(NamenodeQueue, NamenodeLock):
    
    sendMsg(NamenodeQueue, NamenodeLock, 0)
    print("Stopping NameNodes")
    

def cli(NamenodeQueue, NamenodeLock):
    
    sendMsg(NamenodeQueue, NamenodeLock, 1)

    while True:
        cmd = input(">")
        if cmd.strip().lower() == 'exit':
            stopAllNodes(NamenodeQueue, NamenodeLock)
            break
    
if __name__ == "__main__":
    try:
        dfs_setup_config_path = os.path.expandvars(sys.argv[1])
        dfs_setup_config_file = open(dfs_setup_config_path, 'r')
        global dfs_setup_config
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