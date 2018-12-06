import socket
import platform
import time
HOST = "172.20.10.9"
PORT = 5000 # port(same as on server.py)

connection_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
connection_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
connection_socket.connect((HOST, PORT))
print("\n[*] Connected to " +HOST+ " on port " +str(PORT)+ ".\n")

while True:

    connection_socket.send('cool'.encode('utf-8'))
    time.sleep(.5)
connection_socket.close()
