import socket  
import threading
import os
import struct
import hashlib
import time


PORT = 8080
SERVER = socket.gethostbyname(socket.gethostname())


ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FILE_LIST_PATH = os.path.dirname(__file__)
SOURCE_FILE_PATH = os.path.join(FILE_LIST_PATH, "server_files")

CHUNK_SIZE = 1024
ack = set()
server = None
running = True  

def calculate_checksum(data):
    """Calculate checksum of a packet using SHA256."""
    return hashlib.sha256(data).digest()

def mark_acknowledged(seq_num):
    """Mark a sequence number as acknowledged."""
    if seq_num not in ack:
        ack.add(seq_num)

def send_chunks(client_address, filename, start_chunk, end_chunk):
    """Send a specific range of file chunks to the client."""
    file_path = os.path.join(SOURCE_FILE_PATH, filename)

    if not os.path.isfile(file_path):
        server.sendto(b"ERROR: File not found", client_address)
        return

    try:
        file_size = os.path.getsize(file_path)
        with open(file_path, 'rb') as file:
            for seq_num in range(start_chunk, end_chunk + 1):
                if seq_num in ack:
                    continue

                file.seek(seq_num * CHUNK_SIZE)
                data = file.read(min(CHUNK_SIZE, file_size - seq_num * CHUNK_SIZE))
                checksum = calculate_checksum(data)

                packet = struct.pack('I', seq_num) + checksum + data
                server.sendto(packet, client_address)

                if seq_num % 10 == 0:
                    time.sleep(0.0001)
    except Exception as e:
        print(f"[ERROR] Failed to send chunks: {e}")

def handle_client_request(client_address, request):
    """Process the client's request."""
    try:
        request_data = request.decode().split(':')

        if request_data == ["[LIST]"]:
            handle_list_request(client_address)
        elif len(request_data) == 1:
            handle_file_request(client_address, request_data[0])
        elif request_data[0] == "RANGE":
            handle_range_request(client_address, request_data)
        elif request_data[0] == "RESEND":
            handle_resend_request(client_address, request_data)
        elif request_data[0] == "ACK":
            handle_ack_request(request_data)
        else:
            server.sendto(b"ERROR: Invalid request", client_address)
    except Exception as e:
        print(f"[ERROR] Error processing request from {client_address}: {e}")

import json

def format_size(size):
    """Format the file size to KB, MB, or GB."""
    if size < 1024:
        return f"{size:.2f} bytes"
    elif size < 1024 * 1024:
        size_kb = size / 1024
        return f"{size_kb:.2f} KB"
    elif size < 1024 * 1024 * 1024:
        size_mb = size / (1024 * 1024)
        return f"{size_mb:.2f} MB"
    else:
        size_gb = size / (1024 * 1024 * 1024)
        return f"{size_gb:.2f} GB"

def handle_list_request(client_address):
    """Handle the '[LIST]' request."""
    try:
        files = os.listdir(SOURCE_FILE_PATH)
        file_info = []
        
        for file in files:
            file_path = os.path.join(SOURCE_FILE_PATH, file)
            if os.path.isfile(file_path):  
                size = os.path.getsize(file_path)
                formatted_size = format_size(size) 
                file_info.append((file, formatted_size))  
            else:  
                file_info.append((file, "(DIR)"))


        response = "\n".join([f"{file} {size}" for file, size in file_info])
        server.sendto(response.encode(FORMAT), client_address)
        print(f"[SERVER] Sent file list with sizes to {client_address}")
    except Exception as e:
        print(f"[ERROR] Unable to fetch file list: {e}")
        server.sendto(b"ERROR: Unable to fetch file list", client_address)

def handle_file_request(client_address, filename):
    """Handle a file request."""
    file_path = os.path.join(SOURCE_FILE_PATH, filename)
    try:
        if os.path.isfile(file_path):
            file_size = os.path.getsize(file_path)
            server.sendto(str(file_size).encode(FORMAT), client_address)
            print(f"[SERVER] Sent file '{filename}' ({file_size} bytes) to {client_address}")
        else:
            error_msg = f"ERROR: File not found: {filename}"
            server.sendto(error_msg.encode(FORMAT), client_address)
            print(f"[SERVER] {error_msg}")
    except Exception as e:
        print(f"[ERROR] Failed to process file request for {filename}: {e}")
        server.sendto(f"ERROR: Failed to process file request".encode(FORMAT), client_address)
    ack.clear()

def handle_range_request(client_address, request_data):
    """Handle the 'RANGE' request."""
    try:
        _, filename, start_chunk, end_chunk = request_data
        send_chunks(client_address, filename, int(start_chunk), int(end_chunk))
    except ValueError as e:
        print(f"[ERROR] Invalid range request: {request_data}")
        server.sendto(b"ERROR: Invalid range request", client_address)

def handle_resend_request(client_address, request_data):
    """Handle the 'RESEND' request."""
    try:
        _, filename, start_chunk, end_chunk = request_data
        send_chunks(client_address, filename, int(start_chunk), int(end_chunk))
    except ValueError as e:
        print(f"[ERROR] Invalid resend request: {request_data}")
        server.sendto(b"ERROR: Invalid resend request", client_address)

def handle_ack_request(request_data):
    """Handle the 'ACK' request."""
    try:
        _, seq_num = request_data
        mark_acknowledged(int(seq_num))
    except ValueError as e:
        print(f"[ERROR] Invalid ACK request: {request_data}")

def start_server():
    """Initialize and start the UDP server."""
    global running
    global server

    print("[STARTING] Server is starting...")
    print(f"[LISTENING] Server running on {SERVER}:{PORT}")
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(ADDR)
    server.settimeout(1.0)

    connected_clients = set()  

    try:
        while running:
            try:
                request, client_address = server.recvfrom(1024)
                client_ip = client_address[0]  
                if client_ip not in connected_clients:
                    connected_clients.add(client_ip)
                    print(f"[CONNECTION] New client connected: {client_ip}")

                threading.Thread(target=handle_client_request, args=(client_address, request)).start()
            except socket.timeout:
                continue
            except socket.error as e:
                print(f"[ERROR] Socket error: {e}")
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Server shutting down...")
    except Exception as e:
        print(f"[ERROR] Server error: {e}")
    finally:
        if server:
            server.close()
        print("[CLEANUP] Server resources released.")

if __name__ == "__main__":
    try:
        running = True  
        start_server()
    except KeyboardInterrupt:
        running = False
        print("\n[INFO] Shutting down server...")
