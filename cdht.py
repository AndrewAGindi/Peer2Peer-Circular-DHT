# python3 is used
# takes in 5 input arguments: integers between 0-255, 4th is integer value, 5th is between 0 and 1
# first argument is the peer number
# second and third arguments are the next door peers
# fourth is size of packages sent
# fifth is probability of dropping peer
# UDP peer ports are from 50000 + peer number
from socket import *
import os
import threading
import sys
import time

def custom_hash(num):
    return num%256


class PingListener(threading.Thread):
    def __init__(self, num):
        super(PingListener, self).__init__()
        # self.server = socket(AF_INET, SOCK_DGRAM)
        # self.server.bind(("localhost", 50000 + num))
        self.num = num
    def run(self):
        while True:
            print("starting...")
            server = socket(AF_INET, SOCK_DGRAM)
            server.bind(("localhost", 50000 + self.num))
            conn, addr = server.recvfrom(2048)
            print(addr)
            details = conn.decode().split()
            if details[0] == "file":
                print("Received a response message from peer " + details[1] + ", which has the file " + details[2] + ".\nWe now start receiving the file ………")
                

                

                print("The file is received.")
            else:    
                print("A ping " + details[0] + " message was received from Peer " + details[1] + ".")
                server.close()
                if details[0] == "request":
                    server = socket(AF_INET, SOCK_DGRAM)
                    server.connect(("localhost", 50000 + int(details[1])))
                    sender_string = "response " + str(self.num)
                    server.sendall(sender_string.encode())
                    server.close()

class TCPFilePingListener(threading.Thread):
    def __init__(self, tcp_socket, num, neigh, size, drop_rate):
        super().__init__()
        self.tcp_socket = tcp_socket
        self.num = num
        self.neigh = neigh
        self.size = size
        self.drop_rate = drop_rate
    
    def send_file(self, send_socket, file_name):
        file = open(file_name + ".pdf", "rb")
        print(file.read(20))
        print(file.read(20))
        while True:
            package = file.read(self.size)
            send_socket.sendall(package)
            send_socket.settimeout(1)
    
    def run(self):
        conn, addr = self.tcp_socket.accept()
        print("accepted...")
        print(conn)
        print(addr)
        byte_message = conn.recv(1024)
        message = byte_message.decode().split()
        print(message)
        port_num = int(message[1])
        send_socket = socket(AF_INET, SOCK_DGRAM)
        # what about 0?
        if port_num <= self.num:
            print("File " + message[3] + " is here.\nA response message, destined for peer " + message[2] + ", has been sent.")
            send_socket.connect(("localhost", 50000 + int(message[2])))
            send_socket.sendall(("file " + message[2] + " " + message[3]).encode())
            print("We now start sending the file ………")
            self.send_file(send_socket, message[3])
            print("The file is sent.")
        elif self.num == 1:
            print("File " + message[3] + " is here.\nA response message, destined for peer " + message[2] + ", has been sent.")
            send_socket.connect(("localhost", 50000 + int(message[2])))
            send_socket.sendall(("file " + message[2] + " " + message[3]).encode())
            print("We now start sending the file ………")
            self.send_file(send_socket, message[3])
            print("The file is sent.")
        else:
            tcp_send_socket = socket(AF_INET, SOCK_STREAM)
            tcp_send_socket.connect(("localhost", 50000 + self.neigh))
            print("File " + message[3] + " is not stored here. \nFile request message has been forwarded to my successor.")
            tcp_send_socket.sendall(byte_message)
            tcp_send_socket.close()
        send_socket.close()
        self.tcp_socket.close()

peer_number = int(sys.argv[1])
neighbour1 = int(sys.argv[2])
neighbour2 = int(sys.argv[3])
package_size = int(sys.argv[4])
probability = float(sys.argv[5])

thread1 = PingListener(peer_number)
thread1.start()

sender = socket(AF_INET, SOCK_DGRAM)
sender.connect(("localhost", 50000 + neighbour1))
print("sending...")
time.sleep(peer_number/2)
send_string = "request " + str(peer_number)
sender.sendall(send_string.encode())
# sender.send(("request " + str(peer_number)).encode())
sender.close()

sender = socket(AF_INET, SOCK_DGRAM)
sender.connect(("localhost", 50000 + neighbour2))
time.sleep(peer_number/2)
sender.send(send_string.encode())
sender.close()

# setup TCP socket for listening and sending file requests
my_socket = socket(AF_INET, SOCK_STREAM)
my_socket.bind(("localhost", 50000 + peer_number))
my_socket.listen(1)
tcp_listener_thread = TCPFilePingListener(my_socket, peer_number, neighbour1, package_size, probability)
tcp_listener_thread.start()
# how often ping messages displayed? less than 3 mins?
# when receiving a peer request from peer; expected to send a ping response if alive
# when receiving a response to the ping request
while True:
    print("looping...")
    command = input()
    split_command = str.split(command)
    first_command = split_command[0]
    if first_command == "request":
        # requests for file which will be found in the nearest NEXT peer
        # send a file request ping to the peer by going through successive peers
        # peer holding file will then ping requester peer directly
        print("File request message for " + split_command[1] + " has been sent to my successor.")
        sender_socket = socket(AF_INET, SOCK_STREAM)
        file_location = str(custom_hash(int(split_command[1])))
        sender_socket.connect(("localhost", 50000 + neighbour1))
        sender_socket.sendall(("file " + file_location + " " + str(peer_number) + " " + split_command[1]).encode())
        sender_socket.close()