import sys
import socket
import struct
import threading
from time import time
from logHandler import logHandler

# ---- 공통 패킷 형식 (receiver.py와 동일해야 함) ----
HDR = struct.Struct('!iB')              # seq: 4B signed int, type: 1B
TYPE_NAME, TYPE_DATA, TYPE_FIN, TYPE_ACK = 0, 1, 2, 3
RECV_PORT = 10080
MAX_PAYLOAD = 1400 - HDR.size           # 헤더 포함 UDP 페이로드 1400 bytes 이하


def buildPackets(srcFilename, dstFilename):
    """seq 0 = NAME, seq 1..N = 파일 조각 (마지막 조각은 FIN)"""
    with open(srcFilename, 'rb') as f:
        data = f.read()
    chunks = [data[i:i + MAX_PAYLOAD] for i in range(0, len(data), MAX_PAYLOAD)]
    if not chunks:
        chunks = [b'']                  # 빈 파일이어도 FIN 하나는 보냄

    packets = [HDR.pack(0, TYPE_NAME) + dstFilename.encode()]
    for i, chunk in enumerate(chunks):
        ptype = TYPE_FIN if i == len(chunks) - 1 else TYPE_DATA
        packets.append(HDR.pack(i + 1, ptype) + chunk)
    return packets


def fileSender():
    print('sender program starts...', flush=True)
    logProc = logHandler()

    throughput = 0.0
    avgRTT = -1                         # 5단계에서 계산 (-1이면 로그에 안 찍힘)
    ##########################
    packets = buildPackets(srcFilename, dstFilename)
    total = len(packets)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', 0))
    dest = (recvAddr, RECV_PORT)

    # ---- 스레드가 공유하는 상태 ----
    base = 0                            # 가장 오래된 unACKed 패킷 번호
    cond = threading.Condition()        # base 보호 + 대기/깨우기

    # 여기는 ack 스레드
    def ackReceiver():
        nonlocal base
        while True:
            reply, _ = sock.recvfrom(2048)
            ack_num, ptype = HDR.unpack(reply[:HDR.size])
            if ptype != TYPE_ACK:
                continue
            logProc.writeAck(ack_num, 'received')
            with cond:
                if ack_num + 1 > base:          # 새 ACK: 윈도우 전진
                    base = ack_num + 1
                    cond.notify_all()
                if base >= total:
                    return

    logProc.startLogging(srcFilename + '_sending_log.txt')
    startTime = logProc.startTime

    threading.Thread(target=ackReceiver, daemon=True).start()

    # 여기는 main 스레드
    next_seq = 0
    with cond:
        while next_seq < total:
            while next_seq - base >= windowSize:    # 윈도우가 꽉 참
                cond.wait()
            sock.sendto(packets[next_seq], dest)
            logProc.writePkt(next_seq, 'sent')
            next_seq += 1
        while base < total:                         # 마지막 ACK까지 대기
            cond.wait()

    elapsed = time() - startTime
    throughput = total / elapsed
    logProc.writeEnd(throughput, avgRTT)
    print('sender: {} packets, {:.3f}s, goodput {:.2f} pkts/sec'.format(
        total, elapsed, throughput), flush=True)
    sock.close()
    ##########################


if __name__=='__main__':
    recvAddr = sys.argv[1]  #receiver IP address
    windowSize = int(sys.argv[2])   #window size
    srcFilename = sys.argv[3]   #source file name
    dstFilename = sys.argv[4]   #result file name

    fileSender()