import threading
import queue
import re
import numpy as np
import pandas as pd

f=open(input(),encoding="utf8")
def data_clean(txt):
    removeNums=''.join([i for i in txt if not i.isdigit()])
    removeNums=removeNums.lower()
    removePun=re.sub(r"[^a-z\s]+",'',removeNums)
    return ''.join([s for s in removePun.strip().splitlines(True) if s.strip()])


def splitline(text,a):
    lsp = text.splitlines()
    ans=[]
    for i  in np.array_split(lsp,a):
        ans+=[i.tolist()]
    return ans

def sortedlists(list):
    return sorted(list,key=lambda x:x[0])

def mapper(input,queue):
    val=[]
    for i in input:
        for j in i.split():
            val.append([j,1])
    return queue.put(val)

def partition(sortedwords):
    sort1=[]
    sort2=[]
    for i in sortedwords:
        if(i[0][0]<'n'):
            sort1+=[i]
        else:
            sort2+=[i]
    return sort1,sort2

def reducer(part,queue):
    reducerout=[]
    c=1
    for i in range(len(part)-1):
        if(part[i][0]==part[i+1][0]):
            c+=1
        else:
            reducerout+=[[part[i][0],c]]
            c=1
    reducerout+=[[part[-1][0],c]]
    queue.put(reducerout)

def multi_thread_function_red(func,input):
    q1=queue.Queue()
    q2=queue.Queue()
    t1=threading.Thread(target=func,args=(input[0],q1))
    t1.start()
    t2=threading.Thread(target=func,args=(input[1],q2))
    t2.start()
    t1.join()
    t2.join()
    return q1.get(),q2.get()

def multi_thread_function_map(func,input):
    listout=[]
    for i in range(len(input)):
        mq=queue.Queue()
        t=threading.Thread(target=func,args=(input[i],mq))
        t.start()
        t.join()
        listout+=mq.get()
    return listout

def main_function(text):
    cleantext = data_clean(text)
    linessplit = splitline(cleantext,5)
    mapperout=multi_thread_function_map(mapper,linessplit)
    sortedwords = sortedlists(mapperout)
    slicedwords = partition(sortedwords)
    reducerout = multi_thread_function_red(reducer,slicedwords)
    return reducerout[0]+reducerout[1]

output = main_function(f.read())
pd.DataFrame(output).to_csv("Output2.csv",index=False,header = ["Word","Frequency"])