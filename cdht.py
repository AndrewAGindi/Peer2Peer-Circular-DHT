# python3 is used
# takes in 5 input arguments: integers between 0-255, 4th is integer value, 5th is between 0 and 1
# first argument is the peer number
# second and third arguments are the next door peers
# fourth is size of packages sent
# fifth is probability of dropping peer
# UDP peer ports are from 50000 + peer number

# do we need comments?

# what happens if file is 0?
# can ACK's be lost in transmission? using UDP
# what is actully the point of sequence number?
# what happens if i drop my last transmission because it is for breaking loop
# if correct transmission then should be snd and rcv
# if not will be Drop then quit
# expected to be able to handle multiple requests simultaneously?
# as in multiple calls to request at a time? or wait for one to complete before another
from socket import *
import os
import threading
import sys
import time
import random
import signal

def custom_hash(num):
    return num%256

def formatter(event, time, seq_num, size, ack_num):
    return '%-40s %-20.2f %-20s %-20s %-s\n' % (event, time, seq_num, size, ack_num)

class PingListener(threading.Thread):
    def __init__(self, num, start_time, server):
        super(PingListener, self).__init__()
        # self.server = socket(AF_INET, SOCK_DGRAM)
        # self.server.bind(("localhost", 50000 + num))
        self.num = num
        self.start_time = start_time
        self.server = server
    def run(self):
        global preds
        preds = []
        while True:
            conn, addr = self.server.recvfrom(2048)
            details = conn.decode().split()
            if details[0] == "file":
                print("Received a response message from peer " + details[1] + ", which has the file " + details[2] + ".\nWe now start receiving the file ………")
                ack_number = 1
                seq_num = 1
                while True:
                    conn, addr = self.server.recvfrom(2048)
                    if conn == b'':
                        break
                    receiver_log = open("requesting_log.txt", 'a+')
                    
                    end = time.time()
                    timer = end - self.start_time
                    receiver_log.write(formatter("rcv", timer, str(0), str(len(conn)), str(ack_number)))
                    ack_number += len(conn)
                    
                    # sequence number must match. if it doesn't that means the ACK wasn't received

                    self.server.sendto(conn, addr)
                    
                    end = time.time()
                    timer = end - self.start_time
                    receiver_log.write(formatter("snd", timer, str(seq_num), str(len(conn)), str(0)))
                    seq_num += len(conn)

                    file = open("received_file.pdf", 'ab+')
                    file.write(conn)
                    file.close()
                self.server.close()
                print("The file is received.")
                os.system("xdg-open received_file.pdf")
                os.system("xdg-open requesting_log.txt")
                os.system("xdg-open responding_log.txt")
            elif details[0] == "request":
                server = socket(AF_INET, SOCK_DGRAM)
                if int(details[1]) not in preds:
                    preds.append(int(details[1]))
                if len(preds) > 2:
                    preds = []
                sender_string = "response " + str(self.num)
                server.sendto(sender_string.encode(), addr)
                # server.sendall(sender_string.encode())
                # server.close()
                print("A ping request message was received from Peer " + details[1] + ".")
                server.close()

class TCPFilePingListener(threading.Thread):
    def __init__(self, tcp_socket, num, neigh, neigh2, size, drop_rate, start_time):
        super().__init__()
        self.tcp_socket = tcp_socket
        self.num = num
        self.neigh = neigh
        self.neigh2 = neigh2
        self.size = size
        self.drop_rate = drop_rate
        self.start_time = start_time
    
    def send_file(self, send_socket, file_name):
        file = open(file_name + ".pdf", "rb")
        send_socket.settimeout(1)
        package = file.read(self.size)
        sender_log = open("responding_log.txt", "a+")
        sequence_number = 1
        ack_number = 1
        dropped = False
        while True:
            if random.uniform(0, 1) >= self.drop_rate:
                send_socket.sendall(package)
                package = file.read(self.size)
                if package == b'':
                    break
                if dropped:
                    end = time.time()
                    timer = end - self.start_time
                    sender_log.write(formatter("RTX", timer, str(sequence_number), str(self.size), str(0)))
                else:
                    end = time.time()
                    timer = end - self.start_time
                    sender_log.write(formatter("snd", timer, str(sequence_number), str(self.size), str(0)))
                dropped = False
                sequence_number += self.size
            try:
                ACK, addr = send_socket.recvfrom(2048)
                end = time.time()
                timer = end - self.start_time
                sender_log.write(formatter("rcv", timer, str(0), str(self.size), str(ack_number)))
                ack_number += len(ACK)
            except timeout as e:
                if dropped:
                    end = time.time()
                    timer = end - self.start_time
                    sender_log.write(formatter("RTX/Drop", timer, str(sequence_number), str(self.size), str(0)))
                else:
                    end = time.time()
                    timer = end - self.start_time
                    print(str(sequence_number))
                    sender_log.write(formatter("Drop", timer, str(sequence_number), str(self.size), str(0)))
                    dropped = True
        send_socket.sendall(b'')
        file.close()


    def run(self):
        global neighbour1
        global neighbour2
        while True:
            conn, addr = self.tcp_socket.accept()
            byte_message = conn.recv(1024)
            message = byte_message.decode().split()
            port_num = int(message[1])
            send_socket = socket(AF_INET, SOCK_DGRAM)
            if message[0] == "quit":
                print("Peer " + message[1] + " will depart from the network.")
                if self.neigh2 == int(message[1]):
                    neighbour1 = self.neigh
                    neighbour2 = int(message[2])
                    self.neigh2 = neighbour2
                    print("My first successor is now peer " + str(self.neigh))
                    print("My second successor is now peer " + message[2])
                else:
                    neighbour1 = int(message[2])
                    neighbour2 = int(message[3])
                    self.neigh = neighbour1
                    self.neigh2 = neighbour2
                    print("My first successor is now peer " + message[2])
                    print("My second successor is now peer " + message[3])
                # ping through UDP?
                # time.sleep(self.num/2)
                # send_socket.connect(("localhost", 50000 + neighbour1))
                # send_socket.sendall(("request " + str(self.num)).encode())
                # send_socket.close()
                # time.sleep(self.num/2)
                # send_socket = socket(AF_INET, SOCK_DGRAM)
                # send_socket.connect(("localhost", 50000 + neighbour2))
                # send_socket.sendall(("request " + str(self.num)).encode())
                # send_socket.close()
            elif message[0] == "neighbour":
                # print(addr)
                # send_socket.connect(addr)
                if message[1] == '1':
                    conn.send(str(neighbour1).encode())
                elif message[1] == '2':
                    conn.send(str(neighbour2).encode())
                send_socket.close()
            elif port_num <= self.num:
                print("File " + message[3] + " is here.\nA response message, destined for peer " + message[2] + ", has been sent.")
                send_socket.connect(("localhost", 50000 + int(message[2])))
                send_socket.sendall(("file " + str(self.num) + " " + message[3]).encode())
                print("We now start sending the file ………")
                self.send_file(send_socket, message[3])
                print("The file is sent.")
            elif self.num == 1:
                print("File " + message[3] + " is here.\nA response message, destined for peer " + message[2] + ", has been sent.")
                send_socket.connect(("localhost", 50000 + int(message[2])))
                send_socket.sendall(("file " + str(self.num) + " " + message[3]).encode())
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
        # self.tcp_socket.close()

class AliveTester(threading.Thread):
    def __init__(self, peer_num):
        super().__init__()
        self.peer_num = str(peer_num)
    
    def run(self):
        super().run()
        timeout_counter = 0
        timeout_counter2 = 0
        global neighbour1
        global neighbour2
        while True:
            alive_tester = socket(AF_INET, SOCK_DGRAM)
            # alive_tester.connect(("localhost", 50000 + neighbour1))
            alive_tester.sendto(("request " + self.peer_num).encode(), ("localhost", 50000 + neighbour1))
            alive_tester.settimeout(int(self.peer_num)/2)
            try:
                details, addr = alive_tester.recvfrom(2048)
                details = details.decode().split()
                print("A ping " + details[0] + " message was received from Peer " + details[1] + ".")
                timeout_counter = 0
            except timeout as e:
                timeout_counter += 1
            alive_tester.close()
            # time.sleep(int(self.peer_num)/2)
            time.sleep(5)

            alive_tester = socket(AF_INET, SOCK_DGRAM)
            alive_tester.sendto(("request " + self.peer_num).encode(), ("localhost", 50000 + neighbour2))
            alive_tester.settimeout(int(self.peer_num)/2)
            try:
                details, addr = alive_tester.recvfrom(2048)
                details = details.decode().split()
                print("A ping " + details[0] + " message was received from Peer " + details[1] + ".")
                timeout_counter2 = 0
            except timeout as e:
                timeout_counter2 += 1
            alive_tester.close()
            # time.sleep(int(self.peer_num)/2)
            time.sleep(5)
            if timeout_counter > 5 or timeout_counter2 > 5:
                if timeout_counter > 5:
                    print("Peer " + str(neighbour1) + " is no longer alive.")
                    peer_finder = socket(AF_INET, SOCK_STREAM)
                    peer_finder.connect(("localhost", 50000 + neighbour2))
                    peer_finder.sendall(b"neighbour 1")
                    neighbour = peer_finder.recv(1024).decode()
                    neighbour1 = neighbour2
                    neighbour2 = int(neighbour)
                    peer_finder.close()
                    timeout_counter = 0
                if timeout_counter2 > 5:
                    print("Peer " + str(neighbour2) + " is no longer alive.")
                    peer_finder = socket(AF_INET, SOCK_STREAM)
                    peer_finder.connect(("localhost", 50000 + neighbour1))
                    peer_finder.sendall(b"neighbour 2")
                    neighbour = peer_finder.recv(1024).decode()
                    neighbour2 = int(neighbour)
                    peer_finder.close()
                    timeout_counter2 = 0
                print("My first successor is now peer " + str(neighbour1))
                print("My second successor is now peer " + str(neighbour2))
                # time.sleep(int(self.peer_num)/2)
                # ping_socket = socket(AF_INET, SOCK_DGRAM)
                # ping_socket.connect(("localhost", 50000 + neighbour1))
                # ping_socket.sendall(("request " + self.peer_num).encode())
                # ping_socket.close()
                # time.sleep(int(self.peer_num)/2)
                # ping_socket = socket(AF_INET, SOCK_DGRAM)
                # ping_socket.connect(("localhost", 50000 + neighbour2))
                # ping_socket.sendall(("request " + self.peer_num).encode())
                # ping_socket.close()
        

start_time = time.time()
peer_number = int(sys.argv[1])
neighbour1 = int(sys.argv[2])
neighbour2 = int(sys.argv[3])
package_size = int(sys.argv[4])
probability = float(sys.argv[5])

preds = None

server = socket(AF_INET, SOCK_DGRAM)
server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
server.bind(("localhost", 50000 + peer_number))
thread1 = PingListener(peer_number, start_time, server)
thread1.start()

# under the assumption that none of the neighbours have left
# send_string = "request " + str(peer_number)
# sender = socket(AF_INET, SOCK_DGRAM)
# sender.settimeout(2)
# while True:
#     time.sleep(peer_number/2)
#     sender.sendto(send_string.encode(), ("localhost", 50000 + neighbour1))
#     try:
#         details, addr = sender.recvfrom(2048)
#         details = details.decode().split()
#         print("A ping " + details[0] + " message was received from Peer " + details[1] + ".")
#         break
#     except timeout as e:
#         pass
# sender.close()

# sender = socket(AF_INET, SOCK_DGRAM)
# sender.settimeout(2)
# while True:
#     time.sleep(peer_number/2)
#     sender.sendto(send_string.encode(), ("localhost", 50000 + neighbour2))
#     try:
#         details, addr = sender.recvfrom(2048)
#         details = details.decode().split()
#         print("A ping response message was received from Peer " + details[1] + ".")
#         break
#     except timeout as e:
#         pass
# sender.close()

# setup TCP socket for listening and sending file requests
my_socket = socket(AF_INET, SOCK_STREAM)
my_socket.bind(("localhost", 50000 + peer_number))
my_socket.listen(5)
tcp_listener_thread = TCPFilePingListener(my_socket, peer_number, neighbour1, neighbour2, package_size, probability, start_time)
tcp_listener_thread.start()

alive_thread = AliveTester(peer_number)
alive_thread.start()

print("Ready to take user input:")
# how often ping messages displayed? less than 3 mins?
# when receiving a peer request from peer; expected to send a ping response if alive
# when receiving a response to the ping request
while True:
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
    elif first_command == "quit":
        peer_finder = socket(AF_INET, SOCK_STREAM)
        peer_finder.connect(("localhost", 50000 + peer_number))
        peer_finder.sendall(b"neighbour 1")
        # neighbour1 = int(peer_finder.recv(1024).decode())
        conn = peer_finder.recv(1024)
        peer_finder.close()
        neighbour1 = int(conn.decode())

        peer_finder = socket(AF_INET, SOCK_STREAM)
        peer_finder.connect(("localhost", 50000 + peer_number))
        peer_finder.sendall(b"neighbour 2")
        # neighbour2 = int(peer_finder.recv(1024).decode())
        conn = peer_finder.recv(1024)
        neighbour2 = int(conn.decode())
        peer_finder.close()

        quit_socket = socket(AF_INET, SOCK_STREAM)
        quit_socket.connect(("localhost", 50000 + preds[0]))
        quit_socket.sendall(("quit " + str(peer_number) + " " + str(neighbour1) + " " + str(neighbour2)).encode())
        quit_socket.close()
        quit_socket = socket(AF_INET, SOCK_STREAM)
        quit_socket.connect(("localhost", 50000 + preds[1]))
        quit_socket.sendall(("quit " + str(peer_number) + " " + str(neighbour1) + " " + str(neighbour2)).encode())
        quit_socket.close()

        # quit_socket.sendto(("quit " + str(peer_number) + " " + str(neighbour1) + " " + str(neighbour2)).encode(), ("localhost", 50000 + preds[0]))
        # quit_socket.close()
        # quit_socket = socket(AF_INET, SOCK_STREAM)
        # quit_socket.connect(("localhost", 50000 + preds[1]))
        # quit_socket.sendto(("quit " + str(peer_number) + " " + str(neighbour1) + " " + str(neighbour2)).encode(), ("localhost", 50000 + preds[1]))
        # quit_socket.close()
        os.killpg(os.getpid(), signal.SIGKILL)