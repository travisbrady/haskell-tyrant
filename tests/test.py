import socket
import struct

GET = 0x30
MAGIC = 0xc8
adddouble = 0x61
host = 'localhost'
port = 1978


def add_double(sock, key, integ, fract):
    # to get double
    s3 = struct.pack('>BBIQQ', MAGIC, adddouble, len(key), integ, fract)
    s3 += key
    sock.sendall(s3)
    rawcode = sock.recv(1)
    code = ord(rawcode)
    print "CODE", code
    fetch = sock.recv(16)
    print len(fetch), fetch, "uuuu"
    ret = struct.unpack('>QQ', fetch)
    return ret

def get_double(sock, key):
    return add_double(sock, key, 0, 0)

def main():
    sock = socket.socket()
    sock.connect((host, port))
    sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
    key = "blah"
    kl = len(key)
    s = struct.pack('>BBIQQ', MAGIC, adddouble, kl, 10, 5000000000) #10,5000000000
    s += key
    fh = open('hoser', 'wb')
    fh.write(s)
    fh.close()
    sock.sendall(s)
    rawcode = sock.recv(1)
    code = struct.unpack('>B', rawcode)
    fetch = sock.recv(16)
    print struct.unpack('>QQ', fetch)

    bp = add_double(sock, "zero", 0, 0)
    print bp
    #now add
    badd = add_double(sock, "zero", 5, 0)
    print badd
    joey = get_double(sock, "zero")
    print joey

if __name__ == '__main__':
    main()
