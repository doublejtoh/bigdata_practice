import socketserver
import sys

class MyTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        print('클라이언트 접속: {0}.'.format(self.client_address[0]))
        sock = self.request

        rbuff = sock.recv(1024)
        received = str(rbuff, encoding='utf-8')
        print('수신 : {0}'.format(received))

        sock.send(rbuff)
        print('송신 : {0}'.format(received))
        sock.close()

if __name__ == '__main__':
    bindIP = ''
    bindPort = 5000

    server = socketserver.TCPServer((bindIP, bindPort), MyTCPHandler)

    print('서버 시작..')

    server.serve_forever()

