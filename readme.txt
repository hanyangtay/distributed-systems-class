Han Yang, Tay
COMP360 Assignment 3: Distributed Hashtable and Distributed Commit

Configuration Instructions

1.  Edit line 1 of client.py, server.py, viewleader.py to point to your local directory of
    Python 3.
        Line 1:     #! yourLocalDirectoryofPython
        
    To find out the python installation directory using the command line:
        $ which python3
        
2.  Ensure that you have execute permissions for all three files.
    To add permissions using the command line:
        $ chmod a+x client.py
        $ chmod a+x server.py
        $ chmod a+x viewleader.py
        
3.  Know the server address and viewleader address
    (i.e. the network address of the computers which have server.py, viewleader.py)
        $ ipaddr
----------        
Instructions to run the RPC program

1. Execute viewleader.py
        $ ./viewleader.py

2.  Execute server.py
        $ ./server.py
        
    Takes an optional argument:
        --viewleader: specify the viewleader address to connect to
            $./server.py --viewleader ADDRESS
    
3.  Execute client.py with valid arguments

    Takes an optional argument:
        --server: specify the server host to connect to
            $ ./client.py --server ADDRESS
            
        --viewleader: specify the viewleader address to connect to
            $ ./client.py --viewleader ADDRESS

    Takes one of the seven arguments (RPC request):

    ### To server ###
        print: Prints a string on the server.
            $ ./client.py print MESSAGE

        set: Requests the server to set a locally stored key to a specified value
            $ ./client.py set KEY VALUE

        get: Find the value associated with the specified key.
            $ ./client.py get KEY

        query_all_keys: Returns a list of all keys stored in the server.
            $ ./client.py query_all_keys
            
        setr: Requests a distributed set on multiple servers if available
            Executed by distributed commit - all-or-nothing semantics
            $ ./client.py setr KEY VALUE
    
        getr: Find the value associated with the specified distributed key.
            $ ./client.py getr KEY
            
    ### To viewleader ###        
        query_servers: Returns the view leader's active servers and current epoch
        $ ./client.py query_servers

        lock_get: Requests a lock from the viewleader.
            $ ./client.py lock_get LOCKNAME REQUESTER

        lock_release: Releases a lock that it holds.
            $ ./client.py lock_release LOCKNAME REQUESTER
            
     e.g. valid RPC request to a server with address 10.0.2.5
            $ ./client.py --server 10.0.2.5 print Hello world

----------        
Sample Instructions and Expected Behaviour
    Note that all these instructions are tested assuming that client.py, server.py and viewleader.py are executed on the same process. 
    If they are on separate computers, the appropriate optional arguments should be included to specify the server address and the viewleader address.
    
$ ./viewleader.py

$ ./server.py
    Repeat until you have 4 servers running.
    viewleader.py: Should have received 4 notifications about the new servers.
    
$ ./client.py setr KEY VALUE
    Execute this immediately after the last ./server.py
    Other servers are likely to have not received the new epoch change from the viewleader.
    client.py: "Error: Epochs do not match. Setr aborted."
    
$ ./client.py setr KEY VALUE
    Wait for ~10 seconds before executing this RPC.
    3 of the servers should have executed a local set KEY VALUE.
    Note that setr was aborted correctly as it is possible to call setr KEY again after the previous failed attempt.
    
$./client.py getr KEY
    client.py: Key: KEY, Value: VALUE
    
Terminate one of the servers and wait for 30s.
    Rebalancing should occur. 
    Wait for viewleader.py: "Status: Rebalancing is completed".
    Last server without the KEY should have executed the local set KEY VALUE.
    
    Terminate two of the servers. Only one server should remain.

$./client.py getr KEY
    client.py: "Status": "Completed", "Key": KEY, "Value": VAL
    Note that getr still works despite multiple servers failing.
    
$./server.py
    Repeat until you have 4 servers running.
    *wait for rebalancing to be completed between each repeat.

$./client.py setr KEY2 VALUE2
    Execute this immediately after the previous command.
    #If rebalancing has not been completed, 
        client.py: "Status: Servers are rebalancing. Setr aborted."
        
    Only 3 servers should have the key in their stores.
    Test this by executing local RPC: get KEY to all 4 servers.
     
To test if 2PC aborts accurately in the event of a pending commit, uncomment line 196 in client.py and save as client2.py.
$./client2.py setr KEY VALUE
$./client.py setr KEY VALUE
    server.py: "Status: 2PC aborted."
----------
### Bucket Allocator Algorithm ###

# Replica Count: 3
# If there are less than 3 servers, all servers should receive a copy of the key.

# Sorts the view by servers' identity hashes.
# Obtains hash value of the key to be stored.

# Compares it to all the servers in the views, beginning from the bucket with the smallest identity hash. 
# Chooses each bucket where hash(ID) >= hash(key) until 3 buckets are chosen or there are no more buckets to be compared. 
    If there are less than 3 buckets chosen, choose the buckets with smallest hash(ID) until 3 buckets have been chosen.
    
# Returns connection parameter of each server.

# Keys are roughly evenly distribtued #

We assume that the hash function in python returns an even distribution of hash values.
As buckets are allocated by comparing the hash values of unique server IDs and hash values of keys, keys should be distributed evenly among available buckets.

This can be tested by simply running 4-6 servers, and have a client send out multiple (maybe 10) setr requests. Each server should have approximately the same number of keys in their stores.

# Circumstance for rebalancing #
- new server joins
- server leaves / fails
However, when there are only 3 servers in a view, and a server leaves, there is no need for rebalancing as all servers have copies of all available keys.

### Rebalancing Algorithm ###

Consider this circular distributed hash table of servers

(A) - (B) - (C) - (D) - (E) - (F) - (G) - (loops back to A)

---

# Situation 1: new server (*) joins

(A) - (B) - (C) - (*) - (D) - (E) - (F) - (G) - (loops back to A)

The following keys will be involved in rebalancing
and should all be added to (*).

1. keys that are stored as primary copies in B
        Old view: (B) - (C) - (D)
        New view: (B) - (C) - (*)
        Note that they are stored as secondary copies in C.
        These keys need to be removed from D.
        
2. keys that are stored as primary copies in C
        Old view: (C) - (D) - (E)
        New view: (C) - (*) - (D)
        These keys need to be removed from E.

3. keys that should be stored as primary copies in (*)
        Old view: (D) - (E) - (F)
        New view: (*) - (D) - (E)
        Note that they are stored as primary copies in D.
        These keys need to be removed from F.
        
### Server C will execute the following ###
## rebalance_new RPC in server.py
Iterate through all the keys in the store.
        If hash(A) < hash(key) <= hash (B):
        I.e. keys from (1)
        -Send to (*) to be added
        -Send command to (D) to be removed

        If hash(B) < hash(key) <= hash (C):
        I.e. keys from (2)
        -Send to (*) to be added
        -Send command to (E) to be removed

### Server D will execute the following ###
## rebalance_new2 RPC in server.py
Iterate through all the keys in the store.
        If hash(C) < hash(key) <= hash (*):
        I.e. keys from (3)
        -Send to (*) to be added
        -Send command to (F) to be removed

---

# Situation 2: server (D) leaves - denoted  by (XXX) for clarity

(A) - (B) - (C) - (XXX) - (E) - (F) - (G) - (loops back to A)

The following keys will be involved in rebalancing.

1. keys that are stored as primary copies in B
        Old view: (B) - (C) - (XXX)
        New view: (B) - (C) - (E)
        Note that they are stored as secondary copies in C.
        These keys need to be added to E.
        
2. keys that are stored as primary copies in C
        Old view: (C) - (XXX) - (E)
        New view: (C) - (E) - (F)
        These keys need to be added to F

3. keys that should be stored as primary copies in (XXX)
        Old view: (XXX) - (E) - (F)
        New view: (E) - (F) - (G)
        Note that they are stored as secondary copies in E.
        These keys need to be added to G.
        
### Server C will execute the following ###
## rebalance_drop RPC in server.py
Iterate through all the keys in the store.
        If hash(A) < hash(key) <= hash (B):
        I.e. keys from (1)
        -Send to (E) to be added

        If hash(B) < hash(key) <= hash (C):
        I.e. keys from (2)
        -Send to (F) to be added

### Server E will execute the following ###
## rebalance_drop2 RPC in server.py
Iterate through all the keys in the store.
        If hash(C) < hash(key) <= hash (XXX):
        I.e. keys from (3)
        -Send to (G) to be added

---
Note that even with fewer servers, these situations still hold. 
In the RPCs, these servers are identified by their relation to the new/dead server, where 'n' is the new/dead server. 'n-1' refers to the preceding server , 'n+1' refers to the next server in the circular distributed hash table.

---
# Viewleader is always the first node to be informed of any view change.
# If rebalancing is required, it declares a new state where setr/getr RPCS from clients are blocked.

# From the situations outlined above, viewleader always knows which RPCs to send to the servers involved in rebalancing, and sends server information about all the adjacent nodes of the new/dead server.
#Note that viewleader does not have any information about the keys that each server holds. It only sends correct RPCs + part of the view necessary for rebalancing.

# Each server will only copy the keys necessary to be sent to other servers (for removal / addition) from the RPC they received.

# Servers will ping the viewleader once they have finished adding/removing keys from their local store. Once the viewleader has received enough pings (based on the scenario), the viewleader will declare that rebalancing has been completed and all RPCs can be processed as per usual. 

### Distributed Commit Algorithm ###

Client.py sends each server a vote request twoPC_vote.
Server.py replies with a boolean value, its server ID and knowledge of epoch.
    False - if there is a pending 2PC with the same key
    True - otherwise
    
Client.py treats the vote as a ABORT vote if
    - vote has a False boolean value (i.e. there is a pending 2PC with the same key)
    - server ID does not match the server ID generated from the bucket allocator algorithm
    - server's knowledge of epoch does not match the actual epoch from the viewleader
    
Client.py sends a twoPC_commit or twoPC_abort RPC, depending on the vote results.
    
As 2PC is used for the setr function, it uses the local set RPC.


State of work: Complete
If multiple servers leave without waiting for rebalancing to complete, the viewleader may be waiting for a rebalancing ping that never arrives.
Viewleader will then block all setr/getr functions forever.
This may be a good way to indicate that there may be errors in the stores. However, this comes at a price of availability.


Addtional thoughts
This assignment was fun. It would be nice if we could see how the extra credit from both assignments (especially participant-to-participant recovery) were implemented. 