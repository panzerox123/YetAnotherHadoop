import os

def updateLog(log, datanode, block, action):

    
    pass

def writer(datanode, block, data, log):
    with open(os.path.join(datanode, block), 'w') as f:
                f.write(data)
                f.close()
    updateLog(log, datanode, block, 1)

