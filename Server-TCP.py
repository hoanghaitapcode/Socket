import socket
import threading
import os

# Server Configuration
PORT = 8080
HOST = socket.gethostbyname(socket.gethostname())
ADDR = (HOST, PORT)
FORMAT = 'utf-8'
FILE_LIST_PATH = os.path.dirname(__file__)  
FILE_FOLDER_PATH = os.path.join(FILE_LIST_PATH, "files_from_server")
unit = 4096

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

def read_file_list(filename):
    if not os.path.exists(filename):
        print(f"Error: Cannot found: {filename}")
        return "Error: No files exists."
    try:
        with open(filename, "r", encoding='utf-8') as f:
            return f.read().strip()
    except Exception as e:
        print(f"Error: Cannot read file: {e}")
        return "Error: Cannot read file list."

def process_client(conn, addr):
    print(f"***Welcome***{addr} CONNECTED***")
    try:
        file_list = read_file_list(os.path.join(FILE_LIST_PATH, "file_list.txt"))
        conn.send(file_list.encode(FORMAT))
        
        while True:
            data = conn.recv(1024).decode(FORMAT)
            if not data:
                break

            parts = data.split()
            if len(parts) == 1: # Request for file size
                filename = parts[0]
                file_path = os.path.join(FILE_FOLDER_PATH, filename)
                if os.path.exists(file_path):
                    file_size = os.path.getsize(file_path)
                    conn.send(str(file_size).encode(FORMAT))
                    print(f"Server: sent file size for {filename} to {addr}")
                else:
                    conn.send(f"Error: Cannot found: {filename}".encode(FORMAT))
            
            elif len(parts) == 3: # Request for file chunk
                filename, offset_str, len_str = parts
                try:
                    offset = int(offset_str)
                    length = int(len_str)
                    file_path = os.path.join(FILE_FOLDER_PATH, filename)

                    if os.path.exists(file_path):
                        with open(file_path, 'rb') as f:
                            f.seek(offset)
                            remaining = length
                            while remaining > 0:
                                chunk_size = min(unit, remaining)
                                chunk = f.read(chunk_size)
                                if not chunk:
                                    break

                                conn.send(chunk)
                                remaining -= len(chunk)
                    else:
                        conn.send(f"Error: Cannot found: {filename}".encode(FORMAT))
                except Exception as e:
                    conn.send(f"Error: {str(e)}".encode(FORMAT))

            else:
                conn.send("Error: Command doesn't work".encode(FORMAT))
    except Exception as e:
        print(f"Error processing request of client {addr}: {e}")
    finally:
        conn.close()
        print(f"***Goodbye***{addr} disconnected***")

def start_server():
    print("Server is ready...")
    server.listen()
    print(f"Waiting at IP: {HOST} - Port: {PORT}")

    while True:
        try:
            conn, addr = server.accept()
            thread = threading.Thread(target=process_client, args=(conn, addr))
            thread.start()
        except KeyboardInterrupt:
            print("\nServer is shutting down...")
            break
        except Exception as e:
            print(f"Error: {e}")

    server.close()

if __name__ == "__main__":
    start_server()