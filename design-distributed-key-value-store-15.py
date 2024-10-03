# Design a Distributed Key-Value Store
# Build a simple distributed key-value store that supports basic operations like put, get, delete, and replicate data across multiple nodes.
# basic implementation of a distributed key-value store in Python.

import socket
import threading
import pickle


class KeyValueStore:
    def __init__(self, node_name):
        self.store = {}  # Dictionary to store key-value pairs
        self.node_name = node_name  # Name of the current node
        self.nodes = []  # List of other nodes in the system

    def put(self, key, value):
        self.store[key] = value
        print(f"[{self.node_name}] Stored: {key} => {value}")
        self.replicate_data(f"PUT {key} {value}")

    def get(self, key):
        return self.store.get(key, None)

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            print(f"[{self.node_name}] Deleted: {key}")
            self.replicate_data(f"DELETE {key}")

    def replicate_data(self, message):
        for node in self.nodes:
            node.send_message(message)

    def handle_request(self, conn):
        while True:
            try:
                message = conn.recv(1024)
                if not message:
                    break
                command = pickle.loads(message)
                self.process_command(command)
            except Exception as e:
                print(f"[{self.node_name}] Error handling request: {e}")
                break

    def process_command(self, command):
        parts = command.split()
        operation = parts[0]

        if operation == "PUT":
            key, value = parts[1], parts[2]
            self.store[key] = value
            print(f"[{self.node_name}] Replicated PUT: {key} => {value}")
        elif operation == "DELETE":
            key = parts[1]
            if key in self.store:
                del self.store[key]
                print(f"[{self.node_name}] Replicated DELETE: {key}")

    def add_node(self, node):
        self.nodes.append(node)


class Node:
    def __init__(self, host, port, store):
        self.host = host
        self.port = port
        self.store = store

    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"[{self.store.node_name}] Listening on {self.host}:{self.port}...")

        while True:
            conn, addr = server.accept()
            print(f"[{self.store.node_name}] Connected by {addr}")
            threading.Thread(target=self.store.handle_request,
                             args=(conn,)).start()

    def send_message(self, message):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((self.host, self.port))
                client.sendall(pickle.dumps(message))
        except Exception as e:
            print(
                f"[{self.store.node_name}] Error sending message to {self.host}:{self.port}: {e}")


def run_node(node_name, port, other_nodes):
    store = KeyValueStore(node_name)
    current_node = Node('localhost', port, store)

    for node_host, node_port in other_nodes:
        node = Node(node_host, node_port, store)
        store.add_node(node)

    server_thread = threading.Thread(target=current_node.start_server)
    server_thread.start()

    return store


if __name__ == "__main__":
    # Simulating three nodes in the distributed key-value store
    node1 = threading.Thread(target=run_node, args=(
        "Node1", 5001, [('localhost', 5002), ('localhost', 5003)]))
    node2 = threading.Thread(target=run_node, args=(
        "Node2", 5002, [('localhost', 5001), ('localhost', 5003)]))
    node3 = threading.Thread(target=run_node, args=(
        "Node3", 5003, [('localhost', 5001), ('localhost', 5002)]))

    node1.start()
    node2.start()
    node3.start()
