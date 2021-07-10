import socket
from jsonrpclib.SimpleJSONRPCServer import SimpleJSONRPCServer
from jsonrpclib import ServerProxy
import argparse
import threading
import time
import random
import pymysql


conn=None
cursor=None

def get_ip():
    try:
        host_name = socket.gethostname()
        host_ip = socket.gethostbyname(host_name)
        return host_ip
    except:
        print("Unable to get Hostname and IP")
        return None

class Participant:
    def __init__(self,coordinator,timeout,port):
        self.coordinator=coordinator
        self.ip=get_ip()
        self.timeout=timeout
        self.port=port
        self.curr_tid=0
        self.current_query=""
        self.state=""
    
    def execute_query(self):
        cursor.execute(self.current_query)

    def prepare(self):
        time.sleep(self.timeout)
        r=random.uniform(0,1)
        decision="vote-abort" if r>0.9 else "vote-commit"
        if decision=="vote-commit":
            cursor.execute(f'INSERT INTO Participant_Logs (trans_id,op,args) VALUES ({self.curr_tid},"ready","{self.current_query}");')
            conn.commit()
            t1 = threading.Thread(target=self.execute_query, args=())
            t1.start()
        self.coordinator.record_vote(self.ip,decision)
        self.state="ready"

    def commit(self):
        cursor.execute(f'INSERT INTO Participant_Logs (trans_id,op,args) VALUES ({self.curr_tid},"commit","{self.current_query}");')
        conn.commit()
        self.coordinator.record_ack(f'http://{self.ip}:{self.port}')

    def abort(self):
        conn.rollback()
        cursor.execute(f'INSERT INTO Participant_Logs (trans_id,op,args) VALUES ({self.curr_tid},"abort","{self.current_query}");')
        conn.commit()
        self.coordinator.record_ack(f'http://{self.ip}:{self.port}')

    def msg(self,message,data=None):
        if message=="prepare":
            self.state="init"
            self.current_query=data["query"]
            self.curr_tid=data["tid"]
            t1 = threading.Thread(target=self.prepare, args=())
            t1.start()

        elif self.state!="init" and message=="global-abort":
            t1 = threading.Thread(target=self.abort, args=())
            t1.start()
        
        elif  self.state!="init" and message=="global-commit":
            t1 = threading.Thread(target=self.commit, args=())
            t1.start()

        return

if __name__=="__main__":
    conn = pymysql.connect(
        host='127.0.0.1',
        user='krishna', 
        password = "iiit123",
        db='Zoo')
    cursor=conn.cursor()
    parser = argparse.ArgumentParser()
    parser.add_argument("-P", "--port", help = "Enter Port Number")
    parser.add_argument("-T", "--timeout", help = "Enter Number of seconds to sleep")
    args = parser.parse_args()
    port =int(args.port)
    timeout=float(args.timeout)
    print(port)
    coordinator_ip="http://10.3.5.213:8080"
    coordinator = ServerProxy(coordinator_ip)
    handler=Participant(coordinator,timeout,port)
    server = SimpleJSONRPCServer(('0.0.0.0', port))
    server.register_function(handler.msg,"msg")

    server.serve_forever()