#!/usr/bin/python

"""
COMP360 Assignment 3: Distributed Hashtable and Distributed Commit

Server

---
Processes RPC requests from the client continuously.

Types of RPC requests:

    1) Print: Prints a string on the server.
    
    2) Set: Requests the server to set a locally stored key to a specified value
    
    3) Get: Sends the client the value associated with the specified key.
    
    4) Query all keys: Sends the client a list of all keys stored in the server.
    
    5) 2PC Commit: Commits a pending 2PC for a distributed set RPC.
    
    6) 2PC Abort: Aborts a pending 2PC for a distributed set RPC
    
    7) Rebalance keys: when another server joins / leaves the view
        
--- 
Sends a heartbeat RPC with its ID and listening port to view leader at 10s 
intervals.


@author Han Yang, Tay
"""

import argparse
import common
import common2
import json
import uuid
import hashlib
import threading
import time

### Global variables ###


store = {}
pending_2PC = {}
config = {
    "status": True, #active / failed
    "viewleader": None, #viewleader address
    "lastHeartbeat": time.time() - 10, #time of last heartbeat
    "Epoch": None, #knowledge of epoch from viewleader
    "ID": common.hash_key(str(uuid.uuid4())),
    "serverPort": None, #port it is listening on
    "rebalance": False
}



### RPC ###
def init(msg, addr):
    return {}

# set command sets a key in the value store
def set_val(msg, addr):
    print ("Function: Set")
    key = msg["key"]
    val = msg["val"]
    store[key] = val
    print ("Setting key {} to {} in local store".format(key, val))
    return {"Status": "Completed."}

# fetches a key in the value store
def get_val(msg, addr):
    print ("Function: Get")
    key = msg["key"]
    if key in store:
        val = store[key]
        print ("Key: {}, Value: {}".format(key, val))
        return {"Status": "Completed", "Key": key, "Value": val}
    else:
        print ("The key {} does not exist.".format(key))
        return {"Status": "Key doesn't exist."}

# returns all keys in the value store
def query_all_keys(msg, addr):
    print ("Function: query_all_keys")
    keyList = json.dumps(list(store))
    print("Result: {}".format(keyList))
    return {"Result": keyList}

# prints a message
def print_text(msg, addr):
    print ("Function: Print")
    print (" ".join(msg["text"]))
    return {"Status": "Completed."}

#sends heartbeat to viewleader
def heartbeat():
    #for heartbeat call
    args = {
        "vladdr": config["viewleader"],
        "serverID": config["ID"],
        "port": config["serverPort"],
        "cmd": "heartbeat",
        }
    
    #if server has failed, don't connect to viewleader
    if config["status"] == False:
        return None
    
    for port in range(common2.viewleaderLow, common2.viewleaderHigh):
        response = common.send_receive(config["viewleader"], port, args)
        if "Error" in response:
            continue
        if "Failure" in response:
            config["status"] = False
            print(json.dumps(response))
        #updates epoch and last heartbeat received
        config["Epoch"] = response["Epoch"]
        config["lastHeartbeat"] = time.time()
        break
    else:
        print ("\nError: Can't connect to Viewleader.")
    return None

#nil RPC
def tick(msg, addr):
    return {}

### 2PC ###

#votes YES - True, or NO - False to a client coordinator
#sends ID and knowledge of epoch
def twoPC_vote(msg, addr):
    key = msg["key"]
    
    #if server is currently rebalancing or
    #waiting for a pending 2PC for the same key, vote NO
    if key in pending_2PC:
        print("2PC Vote: NO")
        return {"Vote": False, "Epoch": config["Epoch"], "ID": config["ID"]}
    else:
        pending_2PC[key] = addr
        print("2PC Vote: YES")
        return {"Vote": True, "Epoch": config["Epoch"], "ID": config["ID"]}

#does nothing, terminates current 2PC    
def twoPC_abort(msg, addr):
    key = msg["key"]
    pending_2PC.pop(key, None)
    print("2PC aborted.")
    return {"Status": "2PC aborted."}

#sets the keys, terminates current 2PC
def twoPC_commit(msg, addr):
    key = msg["key"]
    msg["cmd"] = "set"
    handler(msg, addr)
    pending_2PC.pop(key, None)
    print("2PC commited.")
    return {"Status": "2PC commited."}

###

### Rebalance ###

#simply copies whole store to the new server
#when no. of servers = 2 or 3
def rebalance_new_simple(msg, addr):
    print("Rebalancing...")
    source = msg["source"]
    target = msg["target"]
    
    args = {"cmd": "rb_addKeys", "keyAdd": store}
    common.send_receive(target["addr"], target["port"], args)
    print("Rebalancing tasks completed.")
    return {"Status": "Sent keys to be added."}

#sends keys to be added to new server / removed from other servers
#in the event that a server joins the view
def rebalance_new(msg, addr):
    servers = msg["servers"]
    print("Rebalancing...")
    #stores keys to be rebalanced in a dict
    #i.e. send to target server
    keyAdd = {} #keys to be copied to n
    keyRemove = {} #keys to be removed from n+1
    keyRemove2 = {} #keys to be removed from n+2

    #if new server should contain secondary/tertiary copies of the keys
    config["rebalance"] = True
    for i in store:
        #for tertiary copies
        keyAdd, keyRemove = rebalance_aux(i, servers["n-2"]["ID"], servers["n-3"]["ID"], 
                      servers["maxN"]["ID"], keyAdd, keyRemove)
                    
        #for secondary copies 
        keyAdd, keyRemove2 = rebalance_aux(i, servers["n-1"]["ID"], servers["n-2"]["ID"], 
                      servers["maxN"]["ID"], keyAdd, keyRemove2)
    config["rebalance"] = False
           
    #sends keys that are to be added to new server
    args = {"cmd": "rb_addKeys", "keyAdd": keyAdd}
    common.send_receive(servers["n"]["addr"], servers["n"]["port"], args)
    
    #sends keys that are to be removed from n + 1
    args = {"cmd": "rb_removeKeys", "keyRemove": keyRemove}
    common.send_receive(servers["n+1"]["addr"], servers["n+1"]["port"], args)

    #sends keys that are to be removed from n + 2
    args = {"cmd": "rb_removeKeys", "keyRemove": keyRemove2}
    common.send_receive(servers["n+2"]["addr"], servers["n+2"]["port"], args)
    
    print("Rebalancing tasks completed.")
    return {"Status": "Sent keys to be added/removed."}

#sends keys to be added to new server / removed from other servers
#in the event that a server joins the view
def rebalance_new2(msg, addr):
    servers = msg["servers"]
    print("Rebalancing...")
    #stores keys to be rebalanced in a dict
    #i.e. send to target server
    keyAdd = {} #keys to be copied to n
    keyRemove = {} #keys to be removed from n+3

    #if new server should contain primary copies of the keys'
    config["rebalance"] = True
    for i in store:
        keyAdd, keyRemove = rebalance_aux(i, servers["n"]["ID"], servers["n-1"]["ID"], 
                      servers["maxN"]["ID"], keyAdd, keyRemove)
    config["rebalance"] = False

    #sends keys that are to be added to new server
    args = {"cmd": "rb_addKeys", "keyAdd": keyAdd}
    common.send_receive(servers["n"]["addr"], servers["n"]["port"], args)

    #sends keys that are to be removed from n
    args = {"cmd": "rb_removeKeys", "keyRemove": keyRemove}
    common.send_receive(servers["n+3"]["addr"], servers["n+3"]["port"], args)

    print("Rebalancing tasks completed.")
    return {"Status": "Sent keys to be added/removed."}

#sends keys to be added to other servers
#in the event that a server leaves the view
def rebalance_drop(msg, addr):
    servers = msg["servers"]
    print("Rebalancing...")
    
    #stores keys to be rebalanced in a dict
    #i.e. send to target server
    keyAdd = {} #keys to be copied to n+1
    keyAdd2 = {} #keys to be copied to n+2

    #if dropped server contains secondary/tertiary copies of the keys
    config["rebalance"] = True
    for i in store:
        #for tertiary copies
        keyAdd = rebalance_aux(i, servers["n-2"]["ID"], servers["n-3"]["ID"], 
                      servers["maxN"]["ID"], keyAdd)
                    
        #for secondary copies        
        keyAdd2 = rebalance_aux(i, servers["n-1"]["ID"], servers["n-2"]["ID"], 
                      servers["maxN"]["ID"], keyAdd)
    config["rebalance"] = False
    
    #sends keys that are stored as tertiary copies in server n
    #to server n+1
    args = {"cmd": "rb_addKeys", "keyAdd": keyAdd}
    common.send_receive(servers["n+1"]["addr"], servers["n+1"]["port"], args)
    
    #sends keys that are stored as secondary copies in server n
    #to server n+2
    args = {"cmd": "rb_addKeys", "keyAdd": keyAdd2}
    common.send_receive(servers["n+2"]["addr"], servers["n+2"]["port"], args)
    
    print("Rebalancing tasks completed.")
    return {"Status": "Sent keys to be added."}

#sends keys to be added to other servers
#in the event that a server leaves the view
def rebalance_drop2(msg, addr):
    servers = msg["servers"]
    print("Rebalancing...")
    
    #stores keys to be rebalanced in a dict
    #i.e. send to target server
    keyAdd = {} #keys to be copied to n+3

    #if dropped server contains primary copies of the keys
    config["rebalance"] = True
    for i in store:
        #for tertiary copies
        keyAdd = rebalance_aux(i, servers["n"]["ID"], servers["n-1"]["ID"], 
                          servers["maxN"]["ID"], keyAdd)
    config["rebalance"] = False
    
    #sends keys that are stored as tertiary copies in server n
    #to server n+1
    args = {"cmd": "rb_addKeys", "keyAdd": keyAdd}
    common.send_receive(servers["n+3"]["addr"], servers["n+3"]["port"], args)
    
    print("Rebalancing tasks completed.")
    return {"Status": "Sent keys to be added."}

#adds keys to local store
def rb_addKeys(msg, addr):
    #prevents changing of keys while server is iterating through the store
    while config["rebalance"]:
        pass
    for k, v in msg["keyAdd"].items():
        current = set_val({"key": k, "val": v}, None)
    thread = threading.Thread(target = rb_end)
    thread.start()
    return {"Status": "Rebalancing completed."}

#removes keys from local store
def rb_removeKeys(msg, addr):
    #prevents changing of keys while server is iterating through the store
    while config["rebalance"]:
        pass
    for k, v in msg["keyRemove"].items():
        store.pop(k, None)
    thread = threading.Thread(target = rb_end)
    thread.start()
    return {"Status": "Rebalancing completed."}

#tells viewleader that one rebalancing task has been completed
def rb_end():
    args = {"cmd": "rb_end"}
    for port in range(common2.viewleaderLow, common2.viewleaderHigh):
        response = common.send_receive(config["viewleader"], port, args)
        if "Error" in response:
            print(json.dumps(response))
            continue
        break
    return {"Status": "Completed rebalancing task."}

#if i is meant to be hashed in server, add key / remove key
def rebalance_aux(i, server, predServer, maxServer, keyAdd, keyRemove = None):
    
    #for edge case if needs to traverse around circular hashtable
    #refer to rebalance algorithm readme
    if predServer == maxServer:
        if common.hash_key(i) > predServer or common.hash_key(i) < server:
            keyAdd[i] = store[i]
            if keyRemove != None:
                keyRemove[i] = store[i]
    else:
        if common.hash_key(i) > predServer and common.hash_key(i) < server:
            keyAdd[i] = store[i]
            if keyRemove != None:
                keyRemove[i] = store[i]
    
    if keyRemove != None:
        return keyAdd, keyRemove

    return keyAdd

### Main Program ###

# RPC dispatcher invokes appropriate function
def handler(msg, addr):
    cmds = {
        "init": init,
        "set": set_val,
        "get": get_val,
        "print": print_text,
        
        "query_all_keys": query_all_keys,
        "timeout": tick,
        
        "twoPC_vote": twoPC_vote,
        "twoPC_abort": twoPC_abort,
        "twoPC_commit": twoPC_commit,
        
        "rb_addKeys": rb_addKeys,
        "rb_removeKeys": rb_removeKeys,
        "rebalance_new": rebalance_new,
        "rebalance_new2": rebalance_new2,
        "rebalance_new_simple": rebalance_new_simple,
        "rebalance_drop": rebalance_drop,
        "rebalance_drop2": rebalance_drop2,        
    }

    #sends heartbeat if ten seconds have elapsed since last heartbeat
    if time.time() - config["lastHeartbeat"] >= 10:
        heartbeat()

    return cmds[msg["cmd"]](msg, addr)

# Server entry point
def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--viewleader', default='localhost')
    args = parser.parse_args()
    config["viewleader"] = args.viewleader
    
    print("Server ID: {}".format(config["ID"]))
    
    for port in range(common2.serverLow, common2.serverHigh):
        config["serverPort"] = port
        result = common.listen(port, handler, 10)
        print result
    print ("Server has failed to bind to any port. Exiting program...")

    
if __name__ == "__main__":
    main()