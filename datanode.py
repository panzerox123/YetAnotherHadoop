import os
import fileinput

def updateLog(log, datanode, block, action):
    log = log + "/datanode"+datanode[-1]
    with open(log, "r") as f:
        contents = f.readlines()
        f.close()
    if(len(contents)+1 < block):
        with open(log, "a") as f:
            f.write(str(block)+", "+str(action))
    else:
        with open(log, "w") as f:
            for line in fileinput.FileInput(log,inplace=1):
                if line[0] == block:
                    line=line.replace(line,str(block)+", "+str(action))


def writer(datanode, block, data, log):
    with open(os.path.join(datanode, "block"+block), 'w') as f:
                f.write(data)
                f.close()
    updateLog(log, datanode, block, 1)


def createNodes(path, num):
    for i in range(num):
        os.mkdir(path+"/datanode"+str(i+1))

