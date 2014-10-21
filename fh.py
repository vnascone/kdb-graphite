#!/usr/bin/python
import socket
import threading
import SocketServer
import time
from Queue import Queue
import q

mq = Queue()
     
class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):


    def handle(self):
        state = []
        data = ''
        while True:
            data = data + self.request.recv(4096)
            state = data.split('\n')

            if not data:
               # Handle this more gracefully later
               break

            data = state.pop(-1)

            [mq.put(x) for x in [ '(' + ';'.join(('`'+x).split()) +')'  for x in state]]
            state = []

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "localhost", 9001

    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()

    c = q.conn(host="127.0.0.1", port=5000)

    while True:
        try:
            data = mq.get()
            #print data
            c('upd['+data+']')
        except:
            print "kdb connection died... retrying"
            try:
                c = q.conn(host="127.0.0.1", port=5000)
                c('upd['+data+']')
                print "successfully reconnected to kdb"
            except:
                print "Unable to reconnect... will re-attempt in 1sec"
                print "dropped msg: %s" % data
            finally:
                time.sleep(1)
