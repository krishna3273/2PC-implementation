from jsonrpclib import ServerProxy

if __name__=="__main__":
    coordinator_ip="http://10.3.5.213:8080"
    coordinator = ServerProxy(coordinator_ip)
    while(True):
        query=input("Enter Query or type 'QUIT' to stop:\n")
        if query=='QUIT':
            break
        coordinator.execute_query(query)
