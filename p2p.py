import os, sys
import logging
import argparse
import socket
import threading
import time
import queue
import json
import math
import hashlib
import copy
import base64
from multiprocessing.pool import ThreadPool

from os import listdir
from os.path import isfile, join

# Some constants
CHUNK_SIZE = 20   # in bytes
PEER_PORT_BASE = 20000

assert sys.version_info >= (3, 6)
cur_dir = os.path.dirname(os.path.realpath(__file__))

###############################################################################
class FileInfo:
    def __init__(self, name, file_length, file_hash, chunks):
        self.name = name
        self.file_length = file_length
        self.file_hash = file_hash
        self.chunks = chunks

    def __repr__(self):
        return f"{self.name}: {self.file_length} bytes"

    def as_dict(self):
        return {
            "name" : self.name,
            "file_length" : self.file_length,
            "file_hash" : self.file_hash,
            "chunks" : self.chunks,
        }


###############################################################################
class PeerInfo:
    def __init__(self, id, connection, files):
        self.id = id
        self.connection = connection

        self.files = files
        for i in range(len(self.files)):
            ff = self.files[i]
            if str(type(ff)) == "<class 'dict'>":
                self.files[i] = FileInfo(**ff)

    def __repr__(self):
        return f"{self.id}: {self.connection}\nfiles: {self.files}"

    def as_dict(self):
        return {
            "id" : self.id,
            "connection" : self.connection,
            "files" : [f.as_dict() for f in self.files],
        }

    def from_dict(self, dictionary):
        pass

###############################################################################
class Protocol:
    header_len = 20
    xfer_batch_max = 2048

    # TODO: UPdate
    @staticmethod
    def send_len(sock, msg, length):
        # logging.debug(f"Sending: [{msg}] of len={len}")
        totalsent = 0
        while totalsent < length:
            sent = sock.send(msg[totalsent:])
            if sent == 0:
                raise RuntimeError("socket send error")
            totalsent = totalsent + sent

    @staticmethod
    def recv_len(sock, length):
        buffer = []
        bytes_recd = 0
        while bytes_recd < length:
            chunk = sock.recv(min(length - bytes_recd, Protocol.xfer_batch_max))
            if chunk == b'':
                raise RuntimeError("socket read error")
            buffer.append(chunk)
            bytes_recd = bytes_recd + len(chunk)
        ret = b''.join(buffer)
        # logging.debug(f"Received: [{ret}]")
        return ret

    @staticmethod
    def send_msg(sock, msg):
        # Encode msg to into bytes
        bytes_arr = msg.encode('utf8')

        # Send message length header
        msg_len = len(bytes_arr)
        Protocol.send_len(sock, msg_len.to_bytes(Protocol.header_len, 'big'), Protocol.header_len)

        # Send actual message
        Protocol.send_len(sock, bytes_arr, msg_len)

    @staticmethod
    def recv_msg(sock):
        # Wait for msg length
        msg_len_bytes = Protocol.recv_len(sock, Protocol.header_len)
        msg_len = int.from_bytes(msg_len_bytes, 'big')

        # Wait for msg
        bytes_arr = Protocol.recv_len(sock, msg_len)
        msg = bytes_arr.decode('utf8')
        return msg

###############################################################################
class Server:
    files = []
    port = 0

    peer_threads = []
    peers = {}
    peers_mutex = threading.Lock()

    ###########################
    def __init__(self, port):
        self.port = port

        self.listen_socket = None

    def start(self):

        logging.info (f"Listening for incoming connections at port: {self.port}")

        # Create a listening socket
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind socket
        self.listen_socket.bind(("", self.port))

        # Listen for connections
        self.listen_socket.listen(5)

        try:
            while True:
                # Accept new connection
                (peer_socket, address) = self.listen_socket.accept()
                logging.info(f"New connection from {address}")

                # Start peer connection thread
                self.peer_threads.append(threading.Thread(target=self.peer_thread, args=(peer_socket, address, self), daemon=True))
                self.peer_threads[-1].start()

        except Exception as e:
            logging.error(e)

            logging.info("Exiting...")
            time.sleep(1)
            self.listen_socket.shutdown(socket.SHUT_RDWR)
            self.listen_socket.close()

    ###########################
    def peer_thread(self, sock, address, server):
        logging.info(f"Starting peer connection thread for {sock}")
        try:
            while True:
                msg = json.loads(Protocol.recv_msg(sock))
                logging.info(f"Recv[{address[1]}]: {msg}")
                
                if msg['op'] == "register":
                    ret = self.register_reply(msg)

                    # Send response
                    Protocol.send_msg(sock, json.dumps(ret))
                elif msg['op'] == "file_list":
                    # return list of files (just names)
                    files_list = self.file_list_reply()

                    # Send response
                    Protocol.send_msg(sock, json.dumps(files_list))
                elif msg['op'] == "file_locations":
                    # Make file recipe
                    recipe = self.file_locations_reply(msg["filename"])

                    # Send response
                    Protocol.send_msg(sock, json.dumps(recipe))
                elif msg['op'] == "chunk_register":
                    ret = self.chunk_register_reply(msg)

                    # Send response
                    Protocol.send_msg(sock, json.dumps(ret))

        except Exception as e:
            # logging.error(e, exc_info=True)
            logging.error(f"Closing server connection to peer {address}")

            sock.shutdown(socket.SHUT_RDWR)
            sock.close()

    def register_reply(self, msg):
        # Register files
        peer_id = msg['info']['id']

        with self.peers_mutex:
            if peer_id in self.peers:
                logging.error(f"Peer ID: {peer_id} is already registered!")
                return {"ret" : -1}
            self.peers[peer_id] = PeerInfo(**msg['info'])

        # Return a dictionary of {filename : success}
        # files_status = {}
        # for f in self.peers[peer_id].files:
        #     files_status[f.name] = True
        return {"ret" : len(self.peers[peer_id].files)}

    def file_list_reply(self):
        # Return files in all peers
        # Only filename and filesize

        files = []
        def add_to_list(filename, length):
            for ff in files:
                if ff[0] == filename:
                    return
            files.append([filename, length])

        with self.peers_mutex:
            for pp in self.peers.values():
                for f in pp.files:
                    add_to_list(f.name, f.file_length)
        # logging.info(files)
        return {"files" : files}

    def file_locations_reply(self, filename):
        # return peers info list (including chunks of files at each peer)

        recipe = {
            "info" : {}
        }
        with self.peers_mutex:
            for pp in self.peers.values():
                for ff in pp.files:
                    if ff.name == filename:
                        recipe["info"]["file_size"] = ff.file_length
                        recipe["info"]["file_hash"] = ff.file_hash
                        recipe["info"]["num_chunks"] = len(ff.chunks)
                        recipe[pp.id] = {
                            "connection" : pp.connection,
                            "chunks" : ff.chunks,
                        }
                        break

        # logging.info(f"Recipe: {str(recipe)}")
        return recipe

    def chunk_register_reply(self, msg):

        with self.peers_mutex:
            pp = self.peers[msg["peer"]]
            logging.info(f"pp={str(pp)}")
            logging.info(f"pp files={str(pp.files)}")

            # Set chunk to True if file record exists for this peer
            for ff in pp.files:
                if ff.name == msg["filename"]:
                    ff.chunks[msg["chunk"]] = [msg["chunk"], True]
                    logging.info(f"Updating file record for peer={pp}: {str(ff)}, {ff.chunks}")
                    return {"ret" : 0}

            # The file is not available on this peer at all
            # make a copy of records
            for pp_cur in self.peers.values():
                for ff in pp_cur.files:
                    if ff.name == msg["filename"]:
                        new_ff = copy.deepcopy(ff)
                        # Set all chunks to false
                        for chunk in new_ff.chunks:
                            chunk[1] = False
                        
                        new_ff.chunks[msg["chunk"]] = [msg["chunk"], True]
                        logging.info(f"Adding new file record for peer={pp}: {str(new_ff)}, {new_ff.chunks}")
                        pp.files.append(new_ff)
                        return {"ret" : 0}

        return {"ret" : -1}

###############################################################################
class Peer:
    info = None

    server = (None, None)
    server_sock = None

    # other_peers = []

    file_recipes = {}

    local_data_dir = ""

    ###########################
    def __init__(self, id, data_dir, server_ip, server_port):
        port = id + PEER_PORT_BASE

        # Get list of files
        file_names = [f for f in listdir(data_dir) if isfile(join(data_dir, f))]
        files = []
        self.files_mutex = {}
        for ff in file_names:
            file_path = f"{data_dir}/{ff}"
            file_hash = self.get_file_hash(file_path)
            file_size = os.path.getsize(file_path)
            num_chunks = math.ceil(float(file_size) / CHUNK_SIZE)
            # All chunks are available for local files
            chunks = [[id, True] for id in range(num_chunks)]

            files.append(FileInfo(
                ff,
                file_size,
                file_hash,
                chunks
            ))

            self.files_mutex[ff] = threading.Lock()
        self.info = PeerInfo(id, [socket.gethostname(), port], files)
        logging.info(f"Created peer: {self.info}")


        self.local_data_dir = data_dir
        logging.info(f"Exposing files in dir: {self.local_data_dir}")

        self.server = (server_ip, server_port)

        # Create incoming peer connection listen thread
        self.listen_thread = threading.Thread(target=self.listen_incoming, args=(), daemon=True)
        self.listen_thread.start()

    def __fini__(self):
        logging.error(f"Closing server connection")
        self.server_sock.shutdown(socket.SHUT_RDWR)
        self.server_sock.close()

    def connect(self):
        logging.info(f"Connecting to server: {self.server}")
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.connect(self.server)

        return self.register_request()

    ###########################
    def listen_incoming(self):
        logging.error(f"Starting listen thread at port: {self.info.connection[1]}")

        # Create a listening socket
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind socket
        self.listen_socket.bind(("", self.info.connection[1]))

        # Listen for connections
        self.listen_socket.listen(5)

        try:
            while True:
                # Accept new connection
                (peer_socket, address) = self.listen_socket.accept()
                logging.info(f"New connection from {address}")

                # Start peer connection thread
                peer_thread = threading.Thread(target=self.other_peer_thread, args=(peer_socket, address, self), daemon=True)
                peer_thread.start()

        except Exception as e:
            logging.error(e)

            logging.info("Exiting...")
            self.listen_socket.shutdown(socket.SHUT_RDWR)
            self.listen_socket.close()

    def other_peer_thread(self, sock, address, server):
        logging.info(f"Starting other peer connection thread for {sock}")
        try:
            while True:
                msg = json.loads(Protocol.recv_msg(sock))
                logging.info(f"Recv [{address[1]}]: {msg}")
                
                if msg['op'] == "download":
                    ret = self.download_reply(msg)

                    # Send response
                    Protocol.send_msg(sock, json.dumps(ret))
                elif msg['op'] == "shutdown":
                    raise Exception("Shutdown signal")

        except Exception as e:
            # logging.error(e, exc_info=True)
            logging.error(f"Closing server connection to other peer {address}")

            sock.shutdown(socket.SHUT_RDWR)
            sock.close()

    ###########################
    def get_file_hash(self, filename):
        with open(filename, "rb") as f:
            file_hash = hashlib.md5()
            chunk = f.read(CHUNK_SIZE)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(CHUNK_SIZE)
        # print(file_hash.hexdigest())
        return file_hash.hexdigest()

    def register_request(self):
        # register with the server
        # ip, port and files are already stored in this class

        # Send request
        Protocol.send_msg(self.server_sock, json.dumps({
                "op" : "register",
                "info" : self.info.as_dict(),
            }))

        # Wait for response
        res = json.loads(Protocol.recv_msg(self.server_sock))
        # print(res)
        # print(len(self.info.files))
        if res["ret"] == len(self.info.files):
            return True
        elif res["ret"] == -1:
            # logging.warning(f"Failed to register peer with server!")
            # Only if duplicate connections are not to be allowed
            # return False
            return True
        else:
            logging.warning(f"Unable to register some files with server!")
            return False


    def file_list_request(self):
        logging.info(f"Sending file list request")
        # Ask for file list from server

        # Send request
        Protocol.send_msg(self.server_sock, json.dumps({
                "op" : "file_list",
            }))

        # Wait for response
        res = json.loads(Protocol.recv_msg(self.server_sock))
        
        return res["files"]

    def file_locations_request(self, filename):
        logging.info(f"Sending file location request: filename={filename}")
        # Ask server for peers info which have requested file

        # Send request
        Protocol.send_msg(self.server_sock, json.dumps({
                "op" : "file_locations",
                "filename" : filename
            }))

        # Wait for response
        res = json.loads(Protocol.recv_msg(self.server_sock))
        self.file_recipes[filename] = res

        # return file recipe
        return res

    def chunk_register_request(self, filename, chunk):
        # Tell the server that I am now source of a chunk
        logging.info(f"Sending chunk update request: filename={filename}, chunk={chunk}")

        # Send request
        Protocol.send_msg(self.server_sock, json.dumps({
                "op" : "chunk_register",
                "peer" : self.info.id,
                "filename" : filename,
                "chunk" : chunk,
            }))

        # Wait for response
        res = json.loads(Protocol.recv_msg(self.server_sock))
        if res["ret"] == 0:
            return True
        else:
            logging.warning(f"Unable to register chunk update with server!")
            return False


    ###########################
    def read_chunk_safe(self, filename, file_size, chunk):
        read_start = chunk * CHUNK_SIZE
        read_size = min(CHUNK_SIZE, file_size - read_start)

        logging.info(f"Reading {read_size} bytes at {read_start} offset")

        with self.files_mutex[filename]:
            with open(f"{self.local_data_dir}/{filename}", "rb") as f:
                f.seek(read_start)
                bytes_arr = f.read(read_size)
                f.close()
        return bytes_arr

    def write_chunk_safe(self, filename, chunk, bytes_arr):
        write_start = chunk * CHUNK_SIZE
        write_size = len(bytes_arr)

        logging.info(f"Writing {write_size} bytes at {write_start} offset")

        # Maybe this is a new file
        if filename not in self.files_mutex:
            self.files_mutex[filename] = threading.Lock()

        with self.files_mutex[filename]:
            with os.fdopen(os.open(f"{self.local_data_dir}/{filename}", os.O_CREAT|os.O_WRONLY), 'wb') as f:
                f.seek(write_start)
                n_bytes = f.write(bytes_arr)
                if n_bytes != write_size:
                    logging.error(f"File write: filename={filename}, request={write_size}, success={n_bytes}")
                f.close()

    def download_reply(self, msg):
        logging.info(f"Uploading chunk: {msg}")

        # Read the file chunk from disk
        bytes_arr = self.read_chunk_safe(msg['filename'], msg['file_size'], msg['chunk'])
        # logging.info(f"{bytes_arr}")
        logging.info(f"Uploading length={len(bytes_arr)}")
        data = base64.b64encode(bytes_arr).decode('utf8')

        return {"data": data}

    def download_chunk(self, filename, file_size, peer, chunk, worker_id):
        logging.info(f"Downloading chunk: worker={worker_id} filename={filename}, chunk={chunk} from peer={peer}")

        # Create connection
        logging.info(f"Connecting to other peer: {peer['connection']}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(tuple(peer["connection"]))

        # Send download request
        Protocol.send_msg(sock, json.dumps({
                "op" : "download",
                "filename" : filename,
                "file_size" : file_size,
                "chunk" : chunk,
            }))

        # Write buffer to output file
        res = json.loads(Protocol.recv_msg(sock))
        bytes_arr = bytes(base64.b64decode(res["data"].encode('utf8')))
        # logging.info(f"res={bytes_arr}")
        self.write_chunk_safe(filename, chunk, bytes_arr)

        # close connection
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()
        pass

    def download_file(self, filename):
        logging.info(f"Downloading filename={filename}")

        # First get file recipe
        recipe = self.file_locations_request(filename)
        logging.info(recipe)
        if (len(recipe["info"]) == 0):
            logging.error(f"File not found in network: filename={filename}")
            return False

        chunks_list = [False] * recipe["info"]['num_chunks']
        # logging.info(f"chunks_list = {chunks_list}")

        worker_lock = threading.Lock()

        def select_next_chunk():

            # Syncronizaion here between workers
            with worker_lock:

                # Update chunks data from server
                recipe_new = self.file_locations_request(filename)

                
                chunk_counter = {}
                for i in range(recipe["info"]['num_chunks']):
                    chunk_counter[i] = 0

                # Get chunks in order of how rare they are
                for pp, details in recipe_new.items():
                    if pp not in ["info"]:
                        for i in range(len(details["chunks"])):
                            if details["chunks"][i][1]:
                                chunk_counter[i] += 1

                # chunk_counter = sorted(chunk_counter)
                sorted_chunks = sorted(chunk_counter.items(), key=lambda kv: kv[1])

                # logging.info(f"chunk_counter={str(chunk_counter)}")
                # logging.info(f"sorted_chunks={str(sorted_chunks)}")
                chunk_order = [kv[0] for kv in sorted_chunks]
                # chunk_order.reverse()

                next_chunk = None
                for i in chunk_order:
                    if not chunks_list[i]:
                        next_chunk = i
                        break

                # No more chunks are pending
                if next_chunk == None:
                    return (None, None)

                chunks_list[next_chunk] = True

            # Select a peer which has this chunk
            for pp, details in recipe_new.items():
                if pp not in ["info"]:
                    if details["chunks"][next_chunk][1]:
                        # the required chunk is available here
                        peer = pp
                        break

            return (peer, next_chunk)

        def worker(worker_id):
            logging.info(f"Starting worker # {worker_id}")

            # While file not complete
            while True:
                # select next chunk
                peer, chunk = select_next_chunk()

                if (peer, chunk) == (None, None):
                    # File is complete
                    break

                # download chunk
                self.download_chunk(filename, recipe["info"]["file_size"], recipe[peer], chunk, worker_id)

                with worker_lock:
                    # Register chunk at server
                    self.chunk_register_request(filename, chunk)
    
            logging.info(f"Exiting download worker # {worker_id}")

        # use thread pool idea
        num_parallel = min(4, recipe["info"]['num_chunks'])
        p = ThreadPool(num_parallel)
        xs = p.map(worker, range(num_parallel))

        # Debug file contents
        # with open(f"{self.local_data_dir}/{filename}", "rb") as f:
        #     bytes_arr = f.read(1024)
        #     f.close()
        # logging.info(bytes_arr)

        # check file hash
        logging.info(f"Downloading complete filename={filename}")
        file_hash = self.get_file_hash(f"{self.local_data_dir}/{filename}")

        logging.info(f"Downloaded file hash = {file_hash}, original hash = {recipe['info']['file_hash']}")
        if file_hash == recipe["info"]['file_hash']:
            logging.info("File hash matched!")
            return True
        else:
            logging.error("File hash NOT matched!")
            return False


###############################################################################
def setup_logging(log_filename, mode='a'):

    os.system("mkdir -p " + os.path.dirname(log_filename))

    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.DEBUG)
    
    fileHandler = logging.FileHandler(log_filename, mode=mode)
    fileHandler.setFormatter(logFormatter)
    fileHandler.setLevel(logging.DEBUG)
    rootLogger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    consoleHandler.setLevel(logging.INFO)
    rootLogger.addHandler(consoleHandler)

    rootLogger.info("Logging setup: log_filename=" + log_filename)
