import json
import sys
import os
import multiprocessing
import namenode
import threading
import time
from subprocess import call
#TODO - initialize the datanodes so they can run independently
class IPC_Pathways():
    def __init__(self, config):
        self.config = config
        self.mainReceiverLoop = True
        self.tmpfileLock = multiprocessing.Lock()
        self.mQueue = multiprocessing.SimpleQueue()
        self.mLock = multiprocessing.Lock()

        self.pnnQueue = multiprocessing.SimpleQueue()
        self.pnnLock = multiprocessing.Lock()
        
        self.snnQueue = multiprocessing.SimpleQueue()
        self.snnLock = multiprocessing.Lock()

        self.PNN = multiprocessing.Process(target=namenode.primary_namenode_thread, args=(self.mQueue, self.mLock, self.pnnQueue, self.pnnLock, self.snnQueue, self.snnLock, self.config, self.tmpfileLock))
        self.PNN.start()
        self.sendMsg(self.pnnQueue, self.pnnLock, [100, None])

        self.SNN = multiprocessing.Process(target=namenode.secondary_namenode_thread, args=(self.mQueue, self.mLock, self.pnnQueue, self.pnnLock, self.snnQueue, self.snnLock, self.config))
        self.SNN.start()

        self.sendMsg(self.snnQueue, self.snnLock, [200, None])

        self.mainReceiver = threading.Thread(target = self.reciever)
        self.mainReceiver.start()

        self.PNNReady = False
        self.SNNReady = False

        while(self.PNNReady == False and self.SNNReady == False):
            continue
        
    def reciever(self):
        while(self.mainReceiverLoop):
            self.receiveMsg()

    def sendMsg(self, queue, lock, data):
        print("Main sent", data[0])
        lock.acquire(block = True)
        queue.put(data)
        lock.release()

    def receiveMsg(self):
        self.mLock.acquire(block = True)
        message = [1, None]
        if(not self.mQueue.empty()):
            message = self.mQueue.get()
            print("Main rcvd", message[0])
        self.mLock.release()
        if(message[0] == 0):
            return 0
        if(message[0] == 100):
            print("Primary namenode started")
            return 100
        elif(message[0] == 101):
            print("Primary namenode cannot access Namenode configuration. Use the 'format' command to format the namenode.")
            return 101
        elif(message[0] == 102):
            print("Primary namenode ready to Sync with Secondary namenode")
            self.PNNReady = True
            return 102
        elif(message[0] == 103):
            return 103
        elif(message[0] == 200):
            print("Secondary namenode started")
            return 200
        elif(message[0] == 202):
            print("Secondary namenode ready to Sync with Primary namenode")
            self.SNNReady = True
            return 202
        elif(message[0] == 204):
            self.PNNReady = False
            self.SNNReady = False
            self.sendMsg(self.pnnQueue, self.pnnLock, [100, None])
            self.SNN = multiprocessing.Process(target=namenode.secondary_namenode_thread, args=(self.mQueue, self.mLock, self.pnnQueue, self.pnnLock, self.snnQueue, self.snnLock, self.config))
            self.SNN.start()
        else:
            return 1

    def formatNamenode(self):
        self.sendMsg(self.pnnQueue, self.pnnLock, [101, None])
    
    def mkdir(self, path):
        self.sendMsg(self.pnnQueue, self.pnnLock, [104, path])
    
    def mkdir_parent(self, path):
        self.sendMsg(self.pnnQueue, self.pnnLock, [105, path])
    
    def rmdir(self, path):
        self.sendMsg(self.pnnQueue, self.pnnLock, [106, path])
    
    def ls(self):
        self.sendMsg(self.pnnQueue, self.pnnLock, [107, None])

    def put(self, src, dest):
        self.sendMsg(self.pnnQueue, self.pnnLock, [108, src, dest])

    def cat(self, path, pr = True):
        self.sendMsg(self.pnnQueue, self.pnnLock, [109, path, pr])
    
    def rm(self,filepath):
        self.sendMsg(self.pnnQueue,self.pnnLock,[110,filepath])

    def stopAllNodes(self):
        print("Stopping Secondary namenode")
        self.sendMsg(self.snnQueue, self.snnLock, [0, None])
        self.SNN.join()
        print("Stopping Primary namenode")
        self.sendMsg(self.pnnQueue, self.pnnLock, [0, None])
        self.PNN.join()
        self.mainReceiverLoop = False
        self.mainReceiver.join()
    
    def mapred(self,file,mapper,reducer,output):
        try:
            tmpfile_path = os.path.join(self.config['path_to_namenodes'], 'tmpfile')
            try:
                os.remove(tmpfile_path)
            except:
                pass
            self.cat(file, False)
            time.sleep(5)
            self.tmpfileLock.acquire()
            with open(tmpfile_path, 'r') as i:
                with open(os.path.join(self.config['path_to_namenodes'], 'mapper'),'w') as o:
                    call('python3 {}'.format(mapper).split(),stdin=i,stdout=o)
            with open(os.path.join(self.config['path_to_namenodes'], 'mapper'), 'r') as i:
                with open(os.path.join(self.config['path_to_namenodes'], 'sorted'),'w') as o:
                    call('sort -k 1,1'.split(),stdin=i,stdout=o)
            with open(os.path.join(self.config['path_to_namenodes'], 'sorted'), 'r') as i:
                with open(os.path.join(self.config['path_to_namenodes'], os.path.basename(output)),'w') as o:
                    call('python3 {}'.format(reducer).split(),stdin=i,stdout=o)
            self.put(os.path.join(self.config['path_to_namenodes'], os.path.basename(output)), os.path.dirname(output))
            self.tmpfileLock.release()
        except Exception as e:
            print("An Error Occured: ", e)

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
        config['path_to_datanodes'] = os.path.expandvars(config['path_to_datanodes'])
        config['path_to_namenodes'] = os.path.expandvars(config['path_to_namenodes'])
        config['datanode_log_path'] = os.path.expandvars(config['datanode_log_path'])
        config['namenode_log_path'] = os.path.expandvars(config['namenode_log_path'])
        config['dfs_setup_config'] = os.path.expandvars(config['dfs_setup_config'])
        config['dfs_setup_config'] = os.path.expandvars(config['dfs_setup_config'])
        config['fs_path'] = os.path.expandvars(config['fs_path'])
        config['namenode_checkpoints'] = os.path.expandvars(config['namenode_checkpoints'])
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

def cli(ipc):
    while True:
        cmd = input(">")
        if cmd.strip().lower() == 'exit':
            ipc.stopAllNodes()
            exit(0)
        if cmd.strip().lower() == 'format':
            ipc.formatNamenode()
        command = cmd.split()
        try:
            if command[0].strip() == 'put':
                ipc.put(command[1], command[2])
            elif command[0].strip() == 'mkdir':
                if(command[1].strip() == '-p'):
                    ipc.mkdir_parent(command[2].strip())
                else:
                    ipc.mkdir(command[1].strip())
            elif command[0].strip() == 'rmdir':
                ipc.rmdir(command[1].strip())
            elif command[0].strip() == 'ls':
                ipc.ls()
            elif command[0].strip() == 'cat':
                ipc.cat(command[1])
            elif command[0].strip()=='mr':
                ipc.mapred(command[1],command[2],command[3],command[4])
            elif command[0].strip()=='rm':
                ipc.rm(command[1])
        except IndexError:
            print('Syntax error')
            continue
    

def main_loop(config):
    ipc = IPC_Pathways(config)
    cli(ipc)

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
    main_loop(dfs_setup_config)
    exit(0)