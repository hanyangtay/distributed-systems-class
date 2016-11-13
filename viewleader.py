#!/usr/bin/python

"""
COMP360 Assignment 3: Distributed Hashtable and Distributed Commit

View leader
    
Processes the following RPC requests from servers continuously.

    1) Heartbeat: Store time of receipt time, address and port of the server
                    If more than 30 seconds have elapsed since heartbeat, server will be marked as failed. Future heartbeats are rejected (until server is restarted with a new ID).
                    
                    returns the server's heartbeat status (accepted / rejected).
                    
Processes the following RPC requests from clients continuously.
    
    1) query_servers: returns the view and current epoch
    
    2) lock_get: if lock is unheld, grant lock to client.
                 if lock is already held by the requester, nothing happens.
                 if lock is held by another client, client is enqueued in a locks' waiter queue.
                 Checks for deadlocks and sends the client a retry status.
                 
    3) lock_release: if client is waiting for the lock, remove client from wait queue
                     if client holds the lock, remove client from queue
                     an error is returned if the lock does not exist or client is not in the queue

If server leaves the current view
    1) Release locks held by server
    2) Informs relevant servers
        a) the dropped server and its adjacent servers
        b) the target server to copy keys to for rebalancing

If server joins the current server
    Informs relevant servers
        a) the new server and its adjacent servers
        b) the target server to copy keys to for rebalancing
        c) the target server to remove keys from for rebalancing

                    
@author Han Yang, Tay
"""

import argparse
import common
import common2
import time
import json
import threading

### Global variables ###
config = {
    "epoch": 0,
    "rebalance": False, #True if servers are undergoing rebalancing
    "pending_rb_tasks": 0, #no. of pending rebalancing tasks
}

servers = {}
locks = {} #indexed by locks
locksTrack = {} #indexed by requesters

### Aux functions ###

### Rebalancing ###


#wraps around the circular distributed hashtable
def f(v, nServers):
    if v >= nServers:
        return v - nServers
    else:
        return v

#returns connection parameters from server information    
def g(s):
    s = {"IP": servers[s]["IP"], "ID": s}
    return common.connectBucket(s)
    
#sends rebalance RPC when server joins 
def rebalance_new(i):
    
    #current view + failed server
    rbServers = sorted([ j for j in servers if servers[j]["Status"] == "Active"])

    #knows the new server in relation to other active servers 
    #in distributed hash table
    index = rbServers.index(i)
    
    if len(rbServers) == 1:
        return None
    
    print("Update: A server has joined the view. Rebalancing...")
    config["rebalance"] = True
    
    #all servers should have the same copy of store
    #if no. of servers <= 3
    if len(rbServers) <= 3:
        config["pending_rb_tasks"] = 1

        source = g(rbServers[index - 1])
        target = g(rbServers[index])
        
        #copies entire store over to new server
        args = {"cmd": "rebalance_new_simple",
                "source": source,
                "target": target}
        common.send_receive(source["addr"], source["port"], args)
        
    else:
        config["pending_rb_tasks"] = 4
        rbIDs = {
            "n-3": index - 3,
            "n-2": index - 2,
            "n-1": index - 1,
            "n": index,
            "n+1": index + 1,
            "n+2": index + 2,
            "n+3": index + 3,
            "maxN": len(rbServers) - 1,
            }
        #function f wraps around the circular distributed hashtable
        #ensures index is not out of range
        #function g returns connection parameters and serverID
        rbIPs = {k: g(rbServers[f(v, len(rbServers))]) for k, v in rbIDs.items()}

        #rebalances keys
        #sends RPC for rebalancing instructions to two servers  
        args = {"cmd": "rebalance_new",
            "servers": rbIPs}
        common.send_receive(rbIPs["n-1"]["addr"], rbIPs["n-1"]["port"], args)
        args["cmd"] = "rebalance_new2"
        common.send_receive(rbIPs["n+1"]["addr"], rbIPs["n+1"]["port"], args)
        
    return None
        
#sends rebalance RPC when server leaves
def rebalance_drop(i):
    #current view + failed server
    rbServers = [ j for j in servers if servers[j]["Status"] == "Active"]
    if i not in rbServers:
        rbServers.append(i)
    rbServers = sorted(rbServers)

    #knows the failed server in relation to other active servers 
    #in distributed hash table
    index = rbServers.index(i)
    
    #only needs to rebalance if original number of servers were > 3
    if len(rbServers) > 3:
        print("Update: A server has left the view. Rebalancing...")
        config["rebalance"] = True
        config["pending_rb_tasks"] = 3
        rbIDs = {
            "n-3": index - 3,
            "n-2": index - 2,
            "n-1": index - 1,
            "n": index,
            "n+1": index + 1,
            "n+2": index + 2,
            "n+3": index + 3,
            "maxN": len(rbServers) - 1,
            }
        #function f wraps around the circular distributed hashtable
        #ensures index is not out of range
        #function g returns connection parameters and serverID
        rbIPs = {k: g(rbServers[f(v, len(rbServers))]) for k, v in rbIDs.items()}

        #rebalances keys that are stored as secondary, tertiary copies in failed server
        #sends RPC for rebalancing instructions to two servers  
        args = {"cmd": "rebalance_drop",
            "servers": rbIPs}
        common.send_receive(rbIPs["n-1"]["addr"], rbIPs["n-1"]["port"], args)
        args["cmd"] = "rebalance_drop2"
        common.send_receive(rbIPs["n+1"]["addr"], rbIPs["n+1"]["port"], args)
        
    return None

###

#scans for newly failed servers
def scanFailedServer():
    currentTime = time.time()
    for i in servers:
        if servers[i]["Status"] == "Active":
            
            #mark server as failed if time elapsed since last heartbeat > 30s
            if currentTime - servers[i]["Last heartbeat"] > 30:
                print("Server {} has failed".format(i))
                ### for rebalancing
                thread = threading.Thread(target = rebalance_drop, args = ([i]))
                thread.start()
                
                servers[i]["Status"] = "Failed"
                
                
                #release any locks held by failed server
                locksReleased = []
                requester = servers[i]["IP"]
                if requester in locksTrack:
                    for lockname in locksTrack[requester]["hold"]:
                        locks[lockname].pop(0)
                        locksReleased.append(lockname)
                    del locksTrack[requester]
                    print("The following locks, which are held by {}, are released: {}".format(requester, json.dumps(locksReleased)))
                    
                #increments epoch by 1 if a server fails
                config["epoch"] += 1
                              
    return None

#detects for deadlock using depth-first search
def deadlockDetection(requester, lockname):
        response = ""
        to_visit = {"requester": [requester], "lock": [lockname]}
        visited = {}
        preds = {}
        preds[requester] = {"requester": requester, "lock": lockname}
        cycle = []
        cycleLock = []

        #not all nodes have been traversed
        while to_visit["requester"] != []:
            v = to_visit["requester"].pop()
            v_lock = to_visit["lock"].pop()

            #deadlock detected
            if v in visited:
                u = preds[v]["requester"]
                u_lock = preds[v]["lock"]
                cycleLock.append(v_lock)
                while u!=v:
                    cycle.append(u)
                    cycleLock.append(u_lock)
                    u = preds[u]["requester"]
                    u_lock = preds[u]["lock"]
                cycle.append(v)
                cycle = list(reversed(cycle))
                cycleLock = list(reversed(cycleLock))
                break

            #adds all processes that hold locks that v is waiting for
            for lockname in locksTrack[v]["wait"]:
                to_visit["requester"].append(locks[lockname][0])
                to_visit["lock"].append(lockname)
                          
                #marks the previous node
                preds[locks[lockname][0]] = {"requester": v, "lock":v_lock}
            visited[v] = True
         
        #prints processes and locks involved in deadlock to viewleader.py
        if cycleLock != []:
            response += "Deadlock detected.\n"   
            for i in range(len(cycle)-1):
                response += "{} is waiting on lock {}, which is held by {}.\n".format(cycle[i], cycleLock[i], cycle[i+1])
            response += "{} is waiting on lock {}, which is held by {}.\n".format(cycle[-1], cycleLock[-1], cycle[0])
        print(response)
        
        return None
    
### RPC ###
def init(msg, addr):
    return {}

#server notifies viewleader that rebalancing tasks have been completed
def rb_end(msg, addr):
    config["pending_rb_tasks"] -= 1
    if config["pending_rb_tasks"] == 0:
        print("Status: Rebalancing is completed")
        config["rebalance"] = False
    return {"Status": "Completed"}

#detects heartbeats from servers
def heartbeat(msg, addr):
    scanFailedServer()
    currentTime = time.time()
    serverID = msg["serverID"]
    serverPort = msg["port"]
    addrPort = str(addr) + ":" + str(serverPort)
                          
    #add new server to view and increment epoch by 1
    if serverID not in servers:
        servers[serverID] = {"IP": addrPort, "Status": "Active", 
                             "Last heartbeat": currentTime}
        config["epoch"] += 1
        
        ### for rebalancing
        thread = threading.Thread(target = rebalance_new, args = ([serverID]))
        thread.start()
        
        print("Added new server from {} to group view.".format(addrPort))
        return {"\nStatus": "Heartbeat accepted.", "Epoch": config["epoch"]}
    else:
        #rejects heartbeat
        if servers[serverID]["Status"] == "Failed":
            print("Rejected heartbeat from {}.".format(addrPort))
            return {"Failure": "Heartbeat rejected.", "Epoch": config["epoch"]}
        #updates last heartbeat received
        else:
            servers[serverID]["Last heartbeat"] = currentTime
            return {"Status": "Heartbeat accepted.", "Epoch": config["epoch"]}

#returns list of active servers and current epoch
def query_servers(msg, addr):
    scanFailedServer()
    print ("Function: query_servers from {}".format(addr))
    activeServers = json.dumps([ {"IP": servers[i]["IP"], "ID": i} 
                        for i in servers if servers[i]["Status"] == "Active"])
    return{"Result": activeServers, "Epoch": config["epoch"], 
           "Rebalance status": config["rebalance"]}

#request for a lock to a shared resource                          
def lock_get(msg, addr):
    scanFailedServer()
    print ("Function: lock_get")
    lockname = msg["lockname"]
    requester = msg["requester"]
    isServer = False
    
    #change requester id if it's a server
    if requester[0] == ":":
        requester = str(addr) + requester
                          
    #add entry to locksTrack if necessary
    if requester not in locksTrack:
        locksTrack[requester] = {
            "hold": [],
            "wait": []
        }
    
    #creates a new lock if necessary
    if lockname not in locks:
        locks[lockname] = []
        
    #adds requester to the lock queue if necessary
    if requester not in locks[lockname]:
        locks[lockname].append(requester)
    
    #grants access if requester is at top of the queue
    if locks[lockname][0] == requester:
        
        #updates locks that are held/requested by the requester
        if lockname not in locksTrack[requester]["hold"]:
            locksTrack[requester]["hold"].append(lockname)
            
        if lockname in locksTrack[requester]["wait"]:
            locksTrack[requester]["wait"].remove(lockname)
                
        print("Status: Lock {} is granted to {}.".format(lockname, requester))
        return {"Status": "Granted".format(lockname)}
    
    else:
        #updates entry in locksTrack, checks for deadlock
        if lockname not in locksTrack[requester]["wait"]:
            locksTrack[requester]["wait"].append(lockname)
        deadlockDetection(requester, lockname)
        print("Status: Denied {} access to lock {}.".format(requester, lockname))
        return{"Retry": "Lock {} is not available.".format(lockname)}
            
#releases a lock
def lock_release(msg, addr):
    lockname = msg["lockname"]
    requester = msg["requester"]
                          
    #adjusts requesterID if requester is a server
    if requester[0] == ":":
        requester = str(addr) + requester
    
    if lockname not in locks:
        print("Status:" "Lock {} doesn't exist".format(lockname))
        return {"Status": "Lock doesn't exist."}
    
    elif locks[lockname] == [] or requester not in locks[lockname]:
        print( "Status: {} is not waiting/holding lock {}.".format(requester, lockname))
        return {"Status": "You are not waiting/holding the lock."}
    
    else:
        locks[lockname].remove(requester)
        #removes requester from queue
        if lockname in locksTrack[requester]["hold"]:  
            locksTrack[requester]["hold"].remove(lockname)
        else:
            locksTrack[requester]["wait"].remove(lockname)
        print("Status: Removed requester from the queue.".format(lockname))
        return {"Status": "Completed."}
    
### Main Program ###

# RPC dispatcher invokes appropriate function                          
def handler(msg, addr):
    cmds = {
        "init": init,
        "heartbeat": heartbeat,
        "query_servers": query_servers,
        "lock_get": lock_get,
        "lock_release": lock_release,
        "rb_end": rb_end,
    }

    return cmds[msg["cmd"]](msg, addr)

#Viewleader entry point                          
def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--viewleader', default='localhost')
    args = parser.parse_args()
    
    for port in range(common2.viewleaderLow, common2.viewleaderHigh):
        result = common.listen(port, handler)
        print result
    print ("Viewleader has failed to bind to any port. Exiting program...")

if __name__ == "__main__":
    main()
    
