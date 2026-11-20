from multiprocessing import Process, Queue
from time import time

_END = None     # sentinel which tells the worker to stop

def logFileWorker(q, filename):
    logFile = open(filename, 'w')

    # Drain the queue until the sentinel arrives.
    # (Do not rely on Queue.empty(); it is not reliable across processes.)
    while True:
        line = q.get()
        if line is _END:
            break
        logFile.write(line)

    logFile.close()
    return 0


class logHandler:
    def __init__(self):
        self.startflag = False
        self.logQueue = Queue()


    def startLogging(self, filename):
        self.filename = filename
        self.loggingProc = Process(target=logFileWorker, args=(self.logQueue, self.filename))
        self.loggingProc.start()
        self.startTime = time()
        self.startflag = True

    def writePkt(self, pktNum, event):
        if self.startflag:
            strToWrite = '{:1.3f} pkt: {} | {}\n'.format(time()-self.startTime, pktNum, event)
            self.logQueue.put(strToWrite)
        else:
            print("WARNING : logging has not been started!")

    def writeAck(self, ackNum, event):
        if self.startflag:
            strToWrite = '{:1.3f} ACK: {} | {}\n'.format(time()-self.startTime, ackNum, event)
            self.logQueue.put(strToWrite)
        else:
            print("WARNING : logging has not been started!")

    def writeCwnd(self, cwnd, ssthresh, state):
        if self.startflag:
            strToWrite = '{:1.3f} cwnd: {:.2f} | ssthresh {:.2f} | {}\n'.format(time()-self.startTime, cwnd, ssthresh, state)
            self.logQueue.put(strToWrite)
        else:
            print("WARNING : logging has not been started!")

    def writeEnd(self, goodput, avgRTT=-1):
        if self.startflag:
            self.logQueue.put('File transfer is finished.\n')
            self.logQueue.put('Goodput : {:.2f} pkts/sec\n'.format(goodput))
            if avgRTT!=-1:
                self.logQueue.put('Average RTT : {:.1f} ms\n'.format(avgRTT))
            self.logQueue.put(_END)
            self.loggingProc.join()
            self.startflag = False
        else:
            print("WARNING : logging has not been started!")
