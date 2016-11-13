#!/usr/bin/python

"""
COMP360 Assignment 3: Distributed Hashtable and Distributed Commit

Client
Sends RPC requests to a server / view leader, which is localhost unless 
otherwise specified.
    
Takes an optional argument:
    --server: specify the server host to connect to
        $ ./client.py --server ADDRESS 
        
Takes an optional argument:
    --viewleader: specify the server host to connect to
        $ ./client.py --viewleader ADDRESS

Takes one of the arguments (RPC request):
--- to Server ---
    print: Prints a string on the server.
        $ ./client.py print MESSAGE
    
    set: Requests the server to set a locally stored key to a specified value
        $ ./client.py set KEY VALUE
    
    get: Find the value associated with the specified key.
        $ ./client.py get KEY
    
    query_all_keys: Returns a list of all keys stored in the server.
        $ ./client.py query_all_keys
        
    setr: Requests to set a distributed key to a specified value
        Executed by distributed commit - all-or-nothing semantics
        $ ./client.py setr KEY VALUE
    
    getr: Find the value associated with the specified distributed key.
        $ ./client.py getr KEY
        
--- to Viewleader ---            
    query_servers: Returns the view leader's active servers and current epoch
        $ ./client.py query_servers

    lock_get: Requests a lock from the viewleader.
        $ ./client.py lock_get LOCKNAME REQUESTER
        
    lock_release: Releases a lock that it holds.
        $ ./client.py lock_release LOCKNAME REQUESTER
        
        
e.g. valid RPC request to a server with address 10.0.2.5
    $ ./client.py --server 10.0.2.5 -print "Hello world"

@author Han Yang, Tay
"""

import common
import common2
import argparse
import time
import sys
import json

#determines which address and ports to connect to by the RPC call
def connectHandler(msg):
    connect = {
        "server": {"addr": msg["server"],
            "portLow": common2.serverLow, 
            "portHigh": common2.serverHigh,
            "cmds": ["set", "get", "print", "query_all_keys"],
            "distributed": False
              },
        
        "viewleader": {"addr": msg["viewleader"],
            "portLow": common2.viewleaderLow, 
            "portHigh": common2.viewleaderHigh,
            "cmds": ["lock_get", "lock_release", "query_servers"],
            "distributed": False
              },
        "distributed": {
            "cmds": ["setr", "getr"],
            "addr": msg["viewleader"],
            "portLow": common2.viewleaderLow, 
            "portHigh": common2.viewleaderHigh,
            "distributed": True
        }
    }
    
    if msg["cmd"] in connect["distributed"]["cmds"]:
        return connect["distributed"]
    if msg["cmd"] in connect["server"]["cmds"]:
        return connect["server"]
    else:
        return connect["viewleader"]

#connect
def connect(connectTo, args):
    portLow = connectTo["portLow"]
    portHigh = connectTo["portHigh"]
    connectTarget = connectTo["addr"]
    
    for port in range(portLow, portHigh):
        print ("Trying to connect to {}:{}...".format(connectTarget, port))
        response = common.send_receive(connectTarget, port, args)
        if "Error" in response:
            continue
        
        #in the event of waiting for lock, repeat RPC every 5 seconds
        if "Retry" in response:
            while "Retry" in response:
                time.sleep(5)
                print("Waiting on lock {}".format(args.lockname))
                for port in range(portLow, portHigh):
                    response = common.send_receive(connectTarget, port, args)
                    if "Error" in response:
                        continue
                    break
                else:
                    print "Client has failed to connect. Exiting program..."
                    sys.exit()
        return response
    else:
        return {"Error": "Failed to connect."}
    
#bucket allocator
def bucket_allocator(key, view):
    buckets = []
    #if < 3 servers, all servers should have the key
    if len(view) <= 3:
        buckets = view
    else:
        #sorts view by servers' identity hash
        sortedView = sorted(view, key=lambda k: k["ID"])
        keyHash = common.hash_key(key)

        #allocates key to at most 3 buckets
        for i in sortedView:
            if len(buckets) == 3:
                break
            if i["ID"] >= keyHash:
                buckets.append({"IP": i["IP"], "ID": i["ID"]})

        #wraps around the circular distributed hashtable
        if len(buckets) < 3:
            for i in range(3 - len(buckets)):
                buckets.append({"IP": sortedView[i]["IP"], "ID": sortedView[i]["ID"]})
            
    bucketAddr = []
    for i in buckets:
        bucket = common.connectBucket(i)
        bucketAddr.append(bucket)
    return bucketAddr
    
# distributed set
def setr(args, connectTo):
    key = args["key"]
    val = args["val"]
    voteResults = []
        
    #RPC query_servers
    args["cmd"] = "query_servers"
    view = connect(connectTo, args)
    currentEpoch = view["Epoch"]
    
    #abort if servers are rebalancing
    if view["Rebalance status"]:
        print("Status: Servers are rebalancing. Setr aborted.")
        return None
    
    view = json.loads(view["Result"])
    
    #finds the appropriate buckets to store key
    buckets = bucket_allocator(key, view)
    if buckets == []:
        print("Status: No servers available. Setr aborted")
        return None
    
    #RPC twoPC_vote to all the buckets
    #store results in voteResults
    args["cmd"] = "twoPC_vote"
    for i in buckets:
        response = common.send_receive(i["addr"], i["port"], args)
        if "Error" in response:
            voteResults.append(False)
            print("Status: Server {} is not responding.".format(i["ID"]))
            
        elif response["ID"] != i["ID"]:
            voteResults.append(False)
            print("Error: Server IDs do not match.")
            
        elif response["Epoch"] != currentEpoch:
            voteResults.append(False)
            print("Error: Epochs do not match.")
            
        else:
            voteResults.append(response["Vote"])
    #time.sleep(10)
    
    if False in voteResults:
        #RPC twoPC_abort
        args["cmd"] = "twoPC_abort"
        for i in buckets:
            response = common.send_receive(i["addr"], i["port"], args)
        print("Error: Setr aborted.")
        
    else:
        #RPC twoPC_commit
        args["cmd"] = "twoPC_commit"
        for i in buckets:
            response = common.send_receive(i["addr"], i["port"], args)
            #prints status of set in each bucket
            print (json.dumps(response), json.dumps(i))
        print("Status: Setr commited.")
        
    return None

#distributed get
def getr(args, connectTo):
    key = args["key"]
    
    #RPC query_servers
    args["cmd"] = "query_servers"
    view = connect(connectTo, args)
    
    #abort if servers are rebalancing
    if view["Rebalance status"]:
        print("Error: Servers are rebalancing. Getr aborted.")
        return None
    
    view = json.loads(view["Result"])
    buckets = bucket_allocator(key, view)

    #RPC Get
    args["cmd"] = "get"
    for i in buckets:
        response = common.send_receive(i["addr"], i["port"], args)
        print(json.dumps(response))
        
        #terminates when a value is returned
        if "Value" in response:
            return None
    
    print("Error: Failed to access key in any server.")
    return None

# Client entry point
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', default='localhost')
    parser.add_argument('--viewleader', default='localhost')

    subparsers = parser.add_subparsers(dest='cmd')
    
    parser_set = subparsers.add_parser('set')
    parser_set.add_argument('key', type=str)
    parser_set.add_argument('val', type=str)

    parser_get = subparsers.add_parser('get')
    parser_get.add_argument('key', type=str)

    parser_print = subparsers.add_parser('print')
    parser_print.add_argument('text', nargs="*")

    parser_query = subparsers.add_parser('query_all_keys')

    parser_queryServers = subparsers.add_parser('query_servers')

    parser_lockGet = subparsers.add_parser('lock_get')
    parser_lockGet.add_argument('lockname', type=str)
    parser_lockGet.add_argument('requester', type=str)

    parser_lockRelease = subparsers.add_parser('lock_release')
    parser_lockRelease.add_argument('lockname', type=str)
    parser_lockRelease.add_argument('requester', type=str)
    
    parser_setr = subparsers.add_parser('setr')
    parser_setr.add_argument('key', type=str)
    parser_setr.add_argument('val', type=str)
    
    parser_getr = subparsers.add_parser('getr')
    parser_getr.add_argument('key', type=str)

    args = vars(parser.parse_args())
    
    connectTo = connectHandler(args)

    if connectTo["distributed"]:
        if args["cmd"] == "setr":
            setr(args, connectTo)
        else:
            getr(args, connectTo)
  
    else:
        #RPC call to a server / viewleader
        response = connect(connectTo, args)
        #print response            
        for p in response.iteritems():
            print ("{}: {}".format(*p))

if __name__ == "__main__":
    main() 

