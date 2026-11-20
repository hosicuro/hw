#!/usr/bin/python

"Run several sender/receiver pairs that share one bottleneck link"

from mininet.net import Mininet
from mininet.topo import Topo
from mininet.util import pmonitor
from mininet.link import TCLink
from mininet.clean import Cleanup
from time import time, sleep
from signal import SIGINT, SIGKILL
from subprocess import STDOUT
import os
import sys


"""
Data flows from tx<i> to rx<i>, and every flow crosses the same bottleneck link.

  tx1 ---+                              +--- rx1
         s1 ======= bottleneck ======= s2
  tx2 ---+                              +--- rx2

By default bottleneck link is set as follow

BandWidth : 10Mbps
delay : 10ms
loss : 0%
queue : 30 packets (netem limit; packets held for the delay are counted too)

usage: sudo python3 execute_fairness.py <window Size> <source Filename> <Result Filename> [number of flows] [start gap]
Every flow sends the same source file, and flow i starts (i-1)*gap seconds after flow 1 (default 2 flows, 5s).
Use a file large enough (e.g. 32MB or more) that all flows send together for a while.
"""

SENDER = 'sender.py'        # change these if your Reno code uses other file names
RECEIVER = 'receiver.py'
TIME_LIMIT = 300            # seconds before every process is stopped


class FairnessTopo(Topo):
    def build(self, n_flows=2, bw_v=10, delay_v="10ms", loss_v=0, queue_v=30):
        "Creating Switch"
        switch1 = self.addSwitch('s1')
        switch2 = self.addSwitch('s2')

        "Senders on s1, receivers on s2; access links are not limited"
        for i in range(1, n_flows + 1):
            self.addLink(self.addHost('tx%d' % i), switch1)
            self.addLink(self.addHost('rx%d' % i), switch2)

        "Shared bottleneck link"
        self.addLink(switch1, switch2,
        bw=bw_v,
        delay=delay_v,
        loss=loss_v,
        max_queue_size=queue_v,
        use_htb=True)


def runMininet():
    Cleanup.cleanup()   #Cleaning up the Mininet before start

    "Get parameter to run the test"
    windowSize = int(sys.argv[1])
    srcFilename = sys.argv[2]
    dstFilename = sys.argv[3]
    numFlows = int(sys.argv[4]) if len(sys.argv) > 4 else 2
    gap = float(sys.argv[5]) if len(sys.argv) > 5 else 5.0

    "You can change the bottleneck link property by modifing these parameter"
    myTopo = FairnessTopo(n_flows=numFlows, bw_v=10, delay_v="10ms", loss_v=0, queue_v=30)  #make test network topology
    net = Mininet(topo=myTopo, link=TCLink) #make Mininet instance
    net.start()     #start Mininet
    print("Starting test...")

    "execute ping for establish switching table"
    net.pingAll(timeout="1") #This code must not be removed

    "Flow i runs in its own directory flow<i>/, so the files and logs of different flows never collide"
    here = os.path.dirname(os.path.abspath(__file__))
    srcName = os.path.basename(srcFilename)
    popens = {}
    t0 = time()
    for i in range(1, numFlows + 1):
        flowDir = os.path.join(here, 'flow%d' % i)
        os.makedirs(flowDir, exist_ok=True)
        link = os.path.join(flowDir, srcName)
        if os.path.lexists(link):
            os.remove(link)
        os.symlink(os.path.abspath(srcFilename), link)

        receiver = net.get('rx%d' % i)
        sender = net.get('tx%d' % i)
        "stderr is merged into stdout so that error messages also show up below"
        popens[receiver] = receiver.popen('python3', os.path.join(here, RECEIVER), cwd=flowDir, stderr=STDOUT)
        "Sender i starts (i-1)*gap seconds later and prints when it actually started"
        popens[sender] = sender.popen('sh', '-c', 'sleep %f; echo start $(date +%%s.%%N); exec python3 "$@"' % ((i - 1) * gap),
                                      'sh', os.path.join(here, SENDER), receiver.IP(), str(windowSize), srcName, dstFilename,
                                      cwd=flowDir, stderr=STDOUT)

    endTime = t0 + TIME_LIMIT
    starts = {}

    "If sender or receiver print something, pmonitor will let you to see it"
    for h, line in pmonitor(popens, timeoutms=500):
        if h:
            if line.startswith('start '):
                starts[h.name] = float(line.split()[1]) - t0
                line = 'started at %.3fs' % starts[h.name]
            print('<%s>: %s' % (h.name, line.rstrip()))
        if time() >= endTime:
            print("Time limit reached, stopping every process")
            for p in popens.values():
                p.send_signal(SIGINT)
            sleep(2)
            "A logging subprocess may survive SIGINT, so kill each process group"
            for p in popens.values():
                try:
                    os.killpg(p.pid, SIGKILL)
                except OSError:
                    pass
            break
    net.stop()  #stop Mininet

    "Start time of each sender, for putting the logs of all flows on one time axis"
    with open(os.path.join(here, 'flow_start_times.txt'), 'w') as f:
        for i in range(1, numFlows + 1):
            f.write('flow%d %.3f\n' % (i, starts.get('tx%d' % i, float('nan'))))
    print("Testing finished")
"runMininet end"


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Error: You need 3 argument.(window Size, source Filename, Result Filename) [number of flows] [start gap]")
    else:
        runMininet()
