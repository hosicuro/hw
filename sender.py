import sys
import socket
import struct
from logHandler import logHandler

HDR = struct.Struct('!iB')
TYPE_NAME, TYPE_DATA, TYPE_FIN, TYPE_ACK = 0, 1, 2, 3
RECV_PORT = 10080


def fileSender():
    print('sender program starts...', flush=True)
    logProc = logHandler()

    throughput = 0.0
    avgRTT = 10.0
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', 0))
    sock.settimeout(5)
    print('sender: bound to port {}'.format(sock.getsockname()[1]), flush=True)

    packet = HDR.pack(0, TYPE_NAME) + dstFilename.encode()
    sock.sendto(packet, (recvAddr, RECV_PORT))
    print('sender: sent NAME packet ({} bytes)'.format(len(packet)), flush=True)

    try:
        reply, _ = sock.recvfrom(2048)
        ack_num, ptype = HDR.unpack(reply[:HDR.size])
        if ptype == TYPE_ACK:
            print('sender: got ACK {}'.format(ack_num), flush=True)
    except socket.timeout:
        print('sender: no ACK within 5s', flush=True)

    sock.close()


if __name__=='__main__':
    recvAddr = sys.argv[1]  #receiver IP address
    windowSize = int(sys.argv[2])   #window size
    srcFilename = sys.argv[3]   #source file name
    dstFilename = sys.argv[4]   #result file name

    fileSender()