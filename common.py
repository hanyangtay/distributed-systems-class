import json
import socket
import struct
import time
import hashlib

MAX_MESSAGE_SIZE = 8192

# Encode and send a message on an open socket
def send(sock, message):
    message = json.dumps(message).encode()

    nlen = len(message)
    if nlen >= MAX_MESSAGE_SIZE:
        return {"Error": "maxmimum message size exceeded"}
    slen = struct.pack("!i", nlen)

    if sock.sendall(slen) is not None:
        return {"Error": "incomplete message"}
    if sock.sendall(message) is not None:
        return {"Error": "incompletely sent message"}

    return {}

# Expect a message on an open socket
def receive(sock):
    nlen = sock.recv(4, socket.MSG_WAITALL)
    if not nlen:
        return {"Error": "can't receive"}

    slen = (struct.unpack("!i", nlen)[0])
    if slen >= MAX_MESSAGE_SIZE:
        return {"Error": "maximum response size exceeded"}
    response = sock.recv(slen, socket.MSG_WAITALL)

    return json.loads(response.decode())

# Sends a message and expects a message
# Parameters
#   host, port - host and port to connect to
#   message - arbitrary Python object to be sent as message
# Return value
#   Response received from server
#   In case of error, returns a dict containing an "Error" key
def send_receive(host, port, message):
    sock = None
    try:
        sock = socket.create_connection((host, port), None)
        if not sock:
            return {"Error": "Can't connect to {}:{}".format(host, port)}

        send_result = send(sock, message)
        if "Error" in send_result:
            return send_result

        receive_result = receive(sock)
        return receive_result

    except ValueError as e:
        return {"Error": "Json encoding error {}".format(e)}
    except socket.error as e:
        return {"Error": "Can't connect to {}:{} because {}".format(host, port, e)}
    finally:
        if sock is not None:
            sock.close()         
            
# A simple RPC server
# Parameters
#   port - port number to listen on for all interfaces
#   handler - function to handle respones, documented below
#   timeout - if not None, after how many seconds to invoke timeout handler
#   vl - a dict containing relevant parameters for heartbeat call
# Return value
#   in case of error, returns a dict with "error" key

#    init: the port has been bound, please perform server initializiation
#    timeout: timeout occurred
#    anything else: RPC command received
# the return value of the handler function is sent as an RPC response
def listen(port, handler, timeout=None):
    bindsock = None
    try:
        bindsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        bindsock.bind(('', port))
        bindsock.listen(100)
        if timeout:
            bindsock.settimeout(timeout)

        if "abort" in handler({"cmd":"init", "port": port}, None):
            return {"error": "listen: abort in init"}

        sock = None
        addr = None
        
        print("Listening on port {}...".format(port))

        while True:
            try:               
                    
                sock, (addr, accepted_port) = bindsock.accept()
                # print("\nAccepting connection from {}".format(addr))
                msg = receive(sock)
                if "Error" in msg:
                    print ("listen: when receiving, {}".format(msg["Error"]))
                    continue

                try:
                    response = handler(msg, addr)
                    if "abort" in response:
                        print ("listen: abort")
                        return response
                        break
                        
                except Exception as e:
                    print ("listen: handler error: {}".format(e))
                    continue
                        
                res = send(sock, response)
                if "Error" in res:
                    print ("listen: when sending, {}".format(res["Error"]))
                    continue
                    
            except socket.timeout:
                if "abort" in handler({"cmd":"timeout"}, None):
                    return {"Error": "listen: abort in timeout"}
    
            except ValueError as e:
                print ("listen: json encoding error {}".format(e))
            except socket.error as e:
                print ("listen: socket error {}".format(e))
            finally:
                if sock is not None:
                    sock.close()
    except socket.error as e:
        return {"Error": "can't bind {}".format(e)}
    finally:
        if bindsock is not None:
            bindsock.close()
            
#hash function
def hash_key(d):
    sha1 = hashlib.sha1(d)
    return int(sha1.hexdigest(), 16)

        
#returns parameters for connecting to server
def connectBucket(i):
    a = i["IP"].split(":")
    bucket = {"addr": a[0],
            "port": int(a[1]),
            "ID": i["ID"]}
    return bucket    