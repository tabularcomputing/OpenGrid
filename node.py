import socket
import pickle
import psutil
import time
import sys
import threading
import subprocess
import requests
import dask
import dask.dataframe as dd

def get_cpu_usage():
    return psutil.cpu_percent(interval=1)

def send_data_to_node(data, node_host, node_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((node_host, node_port))
        s.sendall(data)

def process_data(data_chunk):
    ddf = dd.from_pandas(data_chunk, npartitions=1)
    processed = ddf.map_partitions(lambda df: df.apply(lambda x: x * 2))
    return processed.compute()

def handle_connection(conn):
    while True:
        data = conn.recv(1024)
        if not data:
            break
        received_data = pickle.loads(data)
        if 'data_chunk' in received_data:
            result = process_data(received_data['data_chunk'])
            conn.send(pickle.dumps(result))
        else:
            print(f"Received: {received_data}")
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

def main(interval=3):
    port = 65432
    try:
        threading.Thread(target=start_server, args=(port,)).start()
    except OSError as e:
        print(f"Failed to start the server: {e}")
        sys.exit(1)

    start_ngrok(port)
    
    while True:
        try:
            target_percentage = float(input("Enter target CPU usage percentage: "))
            rebalance_percentage = float(input("Enter rebalance CPU usage percentage: "))
            ngrok_host = input("Enter ngrok host (e.g., 0.tcp.ngrok.io): ")
            ngrok_port_str = input("Enter ngrok port (e.g., 12345): ")
            if not ngrok_port_str.isdigit():
                raise ValueError("Port must be a number")
            ngrok_port = int(ngrok_port_str)

            cpu_usage = get_cpu_usage()
            data = pickle.dumps({'cpu_usage': cpu_usage, 'target': target_percentage, 'rebalance': rebalance_percentage})
            send_data_to_node(data, ngrok_host, ngrok_port)
            time.sleep(interval)
        except ValueError as e:
            print(f"Invalid input: {e}")

if __name__ == "__main__":
    main()
