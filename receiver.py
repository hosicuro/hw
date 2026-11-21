import sys
import os
import socket
import struct
from time import time
from logHandler import logHandler

# ---- 공통 패킷 형식 (sender.py와 동일해야 함) ----
HDR = struct.Struct('!iB')
TYPE_NAME, TYPE_DATA, TYPE_FIN, TYPE_ACK = 0, 1, 2, 3
RECV_PORT = 10080
LINGER = 2.0                    # 전송 완료 후, 이 시간 동안 조용하면 종료
TMP_LOG = 'receiver_log.tmp'    # 파일명을 알기 전까지 쓰는 임시 로그 이름


def fileReceiver():
    print('receiver program starts...', flush=True)
    logProc = logHandler()

    throughput = 0.0
    #########################
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', RECV_PORT))

    expected = 0                # 다음에 순서대로 받아야 할 번호
    buffer = {}                 # seq -> (type, payload) : 순서 어긋난 패킷 보관
    outFile = None
    dstFilename = None
    unique = 0                  # 중복 제외 수신 패킷 수
    endTime = None
    started = False

    while True:
        try:
            packet, addr = sock.recvfrom(2048)
        except socket.timeout:          # 완료 후 LINGER 동안 아무것도 안 옴
            break

        if not started:                 # 첫 패킷 도착 = 로그 0초
            logProc.startLogging(TMP_LOG)
            started = True

        seq, ptype = HDR.unpack(packet[:HDR.size])
        body = packet[HDR.size:]
        logProc.writePkt(seq, 'received')

        # 처음 받는 패킷만 버퍼에 저장 (중복은 무시)
        if seq >= expected and seq not in buffer:
            buffer[seq] = (ptype, body)
            unique += 1

        # expected부터 연속으로 이어진 패킷만 꺼내서 처리
        while expected in buffer:
            ptype_e, body_e = buffer.pop(expected)
            if ptype_e == TYPE_NAME:
                dstFilename = body_e.decode()
                outFile = open(dstFilename, 'wb')
            else:
                outFile.write(body_e)
                if ptype_e == TYPE_FIN:
                    outFile.close()
                    endTime = time()
                    sock.settimeout(LINGER)     # 이제부터 linger 모드
            expected += 1

        # 누적 ACK: 순서대로 받은 마지막 번호 (중복/순서 어긋남에도 항상 보냄)
        ack_num = expected - 1
        sock.sendto(HDR.pack(ack_num, TYPE_ACK), addr)
        logProc.writeAck(ack_num, 'sent')

    throughput = unique / (endTime - logProc.startTime)
    logProc.writeEnd(throughput)
    os.rename(TMP_LOG, dstFilename + '_receiving_log.txt')
    print('receiver: {} unique packets, goodput {:.2f} pkts/sec'.format(
        unique, throughput), flush=True)
    sock.close()
    #########################


if __name__=='__main__':
    fileReceiver()