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
INIT_TIMEOUT = 1.0                      # 명세: 처음 timeout 1초


def buildPackets(srcFilename, dstFilename):
    """seq 0 = NAME, seq 1..N = 파일 조각 (마지막 조각은 FIN)"""
    with open(srcFilename, 'rb') as f:
        data = f.read()
    chunks = [data[i:i + MAX_PAYLOAD] for i in range(0, len(data), MAX_PAYLOAD)]
    if not chunks:
        chunks = [b'']

    packets = [HDR.pack(0, TYPE_NAME) + dstFilename.encode()]
    for i, chunk in enumerate(chunks):
        ptype = TYPE_FIN if i == len(chunks) - 1 else TYPE_DATA
        packets.append(HDR.pack(i + 1, ptype) + chunk)
    return packets


def fileSender():
    print('sender program starts...', flush=True)
    logProc = logHandler()

    throughput = 0.0
    avgRTT = -1                         # 5단계에서 계산
    ##########################
    packets = buildPackets(srcFilename, dstFilename)
    total = len(packets)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', 0))
    dest = (recvAddr, RECV_PORT)

    # ---- 스레드가 공유하는 상태 (모두 cond로 보호) ----
    base = 0                            # 가장 오래된 unACKed 패킷 번호
    next_seq = 0                        # 다음에 새로 보낼 패킷 번호
    timer_start = None                  # 단일 타이머 시작 시각 (None = 꺼짐)
    timeout_val = INIT_TIMEOUT
    send_time = [0.0] * total           # 각 패킷을 마지막으로 보낸 시각
    retx_count = 0
    timeout_count = 0
    cond = threading.Condition()

    def ackReceiver():
        nonlocal base, timer_start
        while True:
            reply, _ = sock.recvfrom(2048)
            ack_num, ptype = HDR.unpack(reply[:HDR.size])
            if ptype != TYPE_ACK:
                continue
            logProc.writeAck(ack_num, 'received')
            with cond:
                if ack_num + 1 > base:              # 새 ACK
                    base = ack_num + 1
                    if base == next_seq:
                        timer_start = None          # 중지: unACKed 없음
                    else:
                        timer_start = time()        # 재시작: unACKed 남음
                    cond.notify_all()
                if base >= total:
                    return

    def sendPacket(seq):
        sock.sendto(packets[seq], dest)
        send_time[seq] = time()

    logProc.startLogging(srcFilename + '_sending_log.txt')
    startTime = logProc.startTime

    threading.Thread(target=ackReceiver, daemon=True).start()

    with cond:
        while base < total:
            now = time()

            # (1) 타이머 만료 -> 가장 오래된 unACKed 패킷(base) 재전송
            if timer_start is not None and now - timer_start >= timeout_val:
                timeout_count += 1
                logProc.writePkt(base, 'timeout (sent at {:.3f}, timer from {:.3f}, timeout {:.3f}s)'.format(
                    send_time[base] - startTime, timer_start - startTime, timeout_val))
                sendPacket(base)
                logProc.writePkt(base, 'retransmitted')
                retx_count += 1
                timer_start = send_time[base]                # 만료 후 재시작
                continue

            # (2) 윈도우에 여유가 있으면 새 패킷 전송
            if next_seq < total and next_seq - base < windowSize:
                sendPacket(next_seq)
                logProc.writePkt(next_seq, 'sent')
                if timer_start is None:
                    timer_start = send_time[next_seq]   # 시작
                next_seq += 1
                continue

            # (3) 할 일 없음 -> ACK 도착 또는 타이머 만료까지 대기
            remaining = timeout_val - (now - timer_start) if timer_start is not None else None
            cond.wait(remaining)

    elapsed = time() - startTime
    throughput = total / elapsed
    logProc.writeEnd(throughput, avgRTT)
    print('sender: {} packets, {:.3f}s, goodput {:.2f} pkts/sec, retx {}, timeout {}'.format(
        total, elapsed, throughput, retx_count, timeout_count), flush=True)
    sock.close()
    ##########################


if __name__=='__main__':
    recvAddr = sys.argv[1]  #receiver IP address
    windowSize = int(sys.argv[2])   #window size
    srcFilename = sys.argv[3]   #source file name
    dstFilename = sys.argv[4]   #result file name

    fileSender()