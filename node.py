import socket
import pickle
import psutil
import time
import sys
import threading
import subprocess
import requests
import random

def get_cpu_usage():
    return psutil.cpu_percent(interval=1)

def send_data_to_node(data, node_host, node_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((node_host, node_port))
        s.sendall(data)
        response = s.recv(4096)
    return pickle.loads(response)

def process_data(data_chunk):
    sorted_chunk = sorted(data_chunk)
    return sorted_chunk

def handle_connection(conn):
    while True:
        data = b''
        while True:
            packet = conn.recv(4096)
            if not packet:
                break
            data += packet
        if not data:
            break
        try:
            received_data = pickle.loads(data)
            processed_data = process_data(received_data)
            conn.send(pickle.dumps(processed_data))
        except Exception as e:
            print(f"Error processing data: {e}")
            break
    conn.close()


def start_server(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', port))
        s.listen()
        print("Node is listening for incoming connections...")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_connection, args=(conn,)).start()

def start_ngrok(port):
    ngrok_cmd = ['ngrok', 'tcp', str(port)]
    subprocess.Popen(ngrok_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"ngrok tunnel is being established for port {port}...")
    time.sleep(2)
    try:
        response = requests.get("http://localhost:4040/api/tunnels").json()
        tcp_tunnel = next(tunnel for tunnel in response['tunnels'] if tunnel['proto'] == 'tcp')
        public_url = tcp_tunnel['public_url']
        ngrok_address = public_url.split("//")[1]
        print(f"ngrok tunnel established at: tcp://{ngrok_address}")
    except Exception as e:
        print(f"Failed to get ngrok tunnel URL: {e}")
        sys.exit(1)

def check_accuracy(original, part1, part2):
    combined = part1 + part2
    combined.sort()
    return combined == original

def main(interval=3):
    port = 65432
    try:
        threading.Thread(target=start_server, args=(port,)).start()
    except OSError as e:
        print(f"Failed to start the server: {e}")
        sys.exit(1)

    start_ngrok(port)
    
    # Generate dataset
    dataset = [random.randint(1, 999) for _ in range(1000)]
    half = len(dataset) // 2
    data_chunk_1 = dataset[:half]
    data_chunk_2 = dataset[half:]

    while True:
        try:
            target_percentage = float(input("Enter target CPU usage percentage: "))
            rebalance_percentage = float(input("Enter rebalance CPU usage percentage: "))
            ngrok_host = input("Enter ngrok host (e.g., 0.tcp.ngrok.io): ")
            ngrok_port_str = input("Enter ngrok port (e.g., 12345): ")
            if not ngrok_port_str.isdigit():
                raise ValueError("Port must be a number")
            ngrok_port = int(ngrok_port_str)

            # Send and receive processed data
            processed_chunk_1 = send_data_to_node(pickle.dumps(data_chunk_1), ngrok_host, ngrok_port)
            processed_chunk_2 = process_data(data_chunk_2)

            # Check result accuracy
            if check_accuracy(dataset, processed_chunk_1, processed_chunk_2):
                print("Data processed correctly.")
            else:
                print("Data processing error.")

            time.sleep(interval)
        except ValueError as e:
            print(f"Invalid input: {e}")

if __name__ == "__main__":
    main()
