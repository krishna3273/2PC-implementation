from jsonrpclib.SimpleJSONRPCServer import SimpleJSONRPCServer
from jsonrpclib import ServerProxy
import threading
import time
import pymysql


handler=None
participants=[]
conns=[]
db_conn=None
cursor=None

class Vote:
    def __init__(self,ip,vote):
        self.ip=ip
        self.vote=vote

class Coordinator:
    def __init__(self,participants,timeout):
        self.participants=participants
        self.timeout=timeout
        self.votes=[]
        self.ack={}
        self.global_decision="global-commit"
        self.query=""
        cursor.execute("SELECT trans_id from Coordinator_Logs ORDER BY trans_id DESC;")
        row=cursor.fetchone()
        if row is not None:
            self.tid=row[0]
        else:
            self.tid=0

    def record_vote(self,ip,msg):
        self.votes.append(Vote(ip,msg))
        return 0

    def record_ack(self,ip):
        self.ack[ip]=True

    def check_votes(self):
        time.sleep(2)
        if len(self.votes)==len(self.participants):
            for vote in self.votes:
                print(f'ip-{vote.ip},decision-{vote.vote}')
                if(vote.vote=="vote-abort"):
                    self.global_decision="global-abort"
        else:
            self.global_decision="global-abort"

        for p in participants:
            self.ack[p]=False

        self.commit_or_abort()
        return

    def check_ack(self):
        time.sleep(1)
        print(self.ack)
        recv=True
        for key in self.ack.keys():
            if self.ack[key]==False:
                recv=False
        if recv:
            print(f"Transaction {'Completed' if self.global_decision=='global-commit' else 'Aborted'} Successfully")
            cursor.execute(f'INSERT INTO Coordinator_Logs (trans_id,op,args) VALUES ({self.tid},"end_of_transaction","None");')
            db_conn.commit()
            self.ack={}
            self.votes=[]
            self.global_decision="global-commit"
            self.query=""
        else:
            for i,participant in enumerate(participants):
                if self.ack[participant]==False:
                     conns[i].msg(self.global_decision)
            t1 = threading.Thread(target=self.check_ack, args=())
            t1.start()
        return

    def commit_or_abort(self):
        cursor.execute(f'INSERT INTO Coordinator_Logs (trans_id,op,args) VALUES  ({self.tid},"{self.global_decision}","None");')
        db_conn.commit()
        for conn in conns:
            conn.msg(self.global_decision)
        t1 = threading.Thread(target=self.check_ack, args=())
        t1.start()
        return



def start_process():
    cursor.execute(f'INSERT INTO Coordinator_Logs (trans_id,op,args) VALUES ({handler.tid},"begin_commit","None");')
    db_conn.commit()
    for i,conn in enumerate(conns):
        # fragment_query=handler.query.replace("Demo",f"Demo{i+1}")
        fragment_query=handler.query
        conn.msg("prepare",{"query":fragment_query,"tid":handler.tid})
    t1 = threading.Thread(target=handler.check_votes, args=())
    t1.start()
    return

def execute_query(query):
    handler.query=query
    handler.tid+=1
    start_process()                                


if __name__=="__main__":
    db_conn = pymysql.connect(
        host='127.0.0.1',
        user='krishna', 
        password = "iiit123",
        db='Zoo')
    cursor=db_conn.cursor()
    with open('participants.txt','r') as participant:
        for line in participant:
            line=line.strip()
            if len(line)==0:
                break
            ip,port=line.split(" ")
            participants.append(f'http://{ip}:{port}')
    for p in participants:
        conns.append(ServerProxy(p))
    handler=Coordinator(participants,5)
    server = SimpleJSONRPCServer(('0.0.0.0', 8080))
    server.register_function(handler.record_vote,'record_vote')
    server.register_function(handler.record_ack,'record_ack')
    server.register_function(execute_query)
    server.serve_forever()
