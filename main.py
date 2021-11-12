import json
import sys
import os
import multiprocessing
import namenode

class NameNode:
    def __init__(self, config):
        self.config = config;
        self.nameNodeQueue = multiprocessing.Queue()
        self.nameNodeLock = multiprocessing.Lock()
        self.nameNode = multiprocessing.Process(target=namenode.nn_loop, args=((self.nameNodeQueue), self.nameNodeLock))

    def startNN(self):
        self.nameNode.start()
    
    def joinNN(self):
        self.nameNode.join()
        
    def sendMsg(self, Data):
        self.nameNodeLock.acquire(block = True)
        self.nameNodeQueue.put(Data)
        self.nameNodeLock.release()

    def receiveMsg(self):
        self.nameNodeLock.acquire(block = True)
        while(self.nameNodeQueue.empty() != True):
            message = self.nameNodeQueue.get()
            if(message == 0):
                break
            else:
                print(message)
        self.nameNodeLock.release()
        if(message == 0):
            return 0
        else:
            return 1

    def stopAllNodes(self):
        self.sendMsg(0)
        print("Stopping NameNodes")
        

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

def start_cli(config):
    primary_nn = NameNode(config)
    primary_nn.startNN()
    cli(primary_nn)
    primary_nn.joinNN()

def cli(primary_nn):
    
    primary_nn.sendMsg(1)

    while True:
        cmd = input(">")
        if cmd.strip().lower() == 'exit':
            primary_nn.stopAllNodes()
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
    start_cli(dfs_setup_config)
    exit(0)