import sys
import socket
import struct
from logHandler import logHandler

HDR = struct.Struct('!iB')          # seq: 4B signed int, type: 1B
TYPE_NAME, TYPE_DATA, TYPE_FIN, TYPE_ACK = 0, 1, 2, 3
RECV_PORT = 10080


def fileReceiver():
    print('receiver program starts...', flush=True)
    logProc = logHandler()

    throughput = 0.0

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', RECV_PORT))
    sock.settimeout(5)

    try:
        packet, addr = sock.recvfrom(2048)
    except socket.timeout:
        print('receiver: no packet within 5s', flush=True)
        sock.close()
        return

    seq, ptype = HDR.unpack(packet[:HDR.size])
    body = packet[HDR.size:]

    if ptype == TYPE_NAME:
        print('receiver: seq={} type=NAME name={} from {}'.format(
            seq, body.decode(), addr), flush=True)
    else:
        print('receiver: unexpected type {}'.format(ptype), flush=True)

    ack = HDR.pack(seq, TYPE_ACK)            # ACK는 헤더만 (payload 없음)
    sock.sendto(ack, addr)                   # 보낸 쪽 주소로 응답
    print('receiver: sent ACK {}'.format(seq), flush=True)

    sock.close()


if __name__=='__main__':
    fileReceiver()