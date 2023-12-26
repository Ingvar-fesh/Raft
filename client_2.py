import socket
import sys
import pickle
import time
import asyncio


def getResponse(address, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((address, int(port)))
            sock.listen(1)
            conn, addr = sock.accept()
            with conn:
                data = conn.recv(1024)
                while data == b'':
                    print(data)
                    data = conn.recv(1024)
                return pickle.loads(data)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    print("You have to command:\n1)get key param\n2)set key param")
    address = sys.argv[1]
    port = int(sys.argv[2])
    port_server = int(sys.argv[3])

    add = (address, port)
    while True:
        data = input().split(" ")
        if data[0] == "get" or data[0] == "pop":
            package = {
                'type': 'CR',
                'client': add,
                'request': f'{data[0]} {data[1]}',
            }
        elif data[0] == "set" or data[0] == "lock" or data[0] == "unlock":
            package = {
                'type': 'CR',
                'client': add,
                'request': f'{data[0]} {data[1]} {data[2]}',
            }

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((address, port_server))
                sock.sendall(pickle.dumps(package, pickle.HIGHEST_PROTOCOL))
                res = getResponse(address, port)
            except Exception as err:
                print(f'unable to connect {address} {err}')
                res = 'network error'
            print(res)
