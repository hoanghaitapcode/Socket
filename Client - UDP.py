import socket
import struct
import os
import threading
import time
import hashlib
import shutil
import sys


HOST = input("Enter HOST IP: ")
PORT_NUM = int(input("Port: "))
ADDRESS = (HOST, PORT_NUM)
ENCODING = "utf-8"
BASE_DIRECTORY = os.path.dirname(__file__)
INPUT_FILE = os.path.join(BASE_DIRECTORY, "input.txt")
OUTPUT_DIRECTORY = os.path.join(BASE_DIRECTORY, "output")

BUFFER_SIZE = 1024

thread_lock = threading.Lock()
files_pending = []
files_downloaded = set()

if not os.path.exists(OUTPUT_DIRECTORY):
    os.makedirs(OUTPUT_DIRECTORY)
if not os.path.exists(INPUT_FILE):
        with open(INPUT_FILE, "w") as file:
            file.write("")

def compute_checksum(data):
    return hashlib.sha256(data).digest()

def watch_input_file():
    while True:
        if os.path.exists(INPUT_FILE):
            with open(INPUT_FILE, "r") as file:
                seen = set()
                lines = []
                for line in file:
                    line = line.strip()
                    if line:
                        # Tách tên file và dung lượng
                        file_name = line.split()[0]  # Lấy phần tử đầu tiên là tên file
                        if file_name not in seen:
                            lines.append(file_name)
                            seen.add(file_name)
            with thread_lock:
                new_requests = [line for line in lines if line not in files_downloaded and line not in files_pending]
                files_pending.extend(new_requests)

        time.sleep(5)



def combine_parts(partial_files, final_file_path):
    try:
        with open(final_file_path, 'wb') as final_file:
            for partial_file in partial_files:
                try:
                    with open(partial_file, 'rb') as part:
                        shutil.copyfileobj(part, final_file, BUFFER_SIZE)
                except FileNotFoundError:
                    print(f"Part file not found: {partial_file}")
                except Exception as e:
                    print(f" Error reading part file {partial_file}: {e}")
        print(f"\nCombined all parts into {final_file_path}")
    except Exception as e:
        print(f"Failed to create final file {final_file_path}: {e}")

def receive_file_chunks(server_address, file_name, start_chunk, end_chunk, output_file, bytes_remaining, part_index, stop_signal, progress_tracker, thread_lock):
    total_bytes = min(bytes_remaining, (end_chunk - start_chunk + 1) * BUFFER_SIZE)
    bytes_received = 0

    if stop_signal.is_set():
        return

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_sock:
            client_sock.settimeout(2.0)

            request_message = f"RANGE:{file_name}:{start_chunk}:{end_chunk}".encode()
            client_sock.sendto(request_message, server_address)

            buffer = {}
            chunks_received = set()
            expected_chunks = set(range(start_chunk, end_chunk + 1))

            with open(output_file, 'wb') as part_file:
                while chunks_received != expected_chunks:
                    if stop_signal.is_set():
                        break

                    try:
                        packet, _ = client_sock.recvfrom(1024 + BUFFER_SIZE)
                        seq_num, = struct.unpack('I', packet[:4])
                        checksum = packet[4:36]
                        data = packet[36:]


                        if compute_checksum(data) == checksum:
                            if seq_num not in chunks_received:
                                buffer[seq_num] = data
                                chunks_received.add(seq_num)
                                bytes_received += len(data)


                                update_progress(progress_tracker, part_index, bytes_received, total_bytes, thread_lock)


                                while start_chunk in buffer:
                                    part_file.write(buffer.pop(start_chunk))
                                    start_chunk += 1


                                ack_message = f"ACK:{seq_num}".encode()
                                client_sock.sendto(ack_message, server_address)
                        else:

                            resend_message = f"RESEND:{file_name}:{seq_num}:{seq_num}".encode()
                            client_sock.sendto(resend_message, server_address)

                    except socket.timeout:

                        missing_chunks = expected_chunks - chunks_received
                        if missing_chunks:
                            resend_message = f"RESEND:{file_name}:{min(missing_chunks)}:{max(missing_chunks)}".encode()
                            client_sock.sendto(resend_message, server_address)

    except Exception as e:
        print(f"\nError in part {part_index + 1}: {e}")
    finally:

        if bytes_received == total_bytes:
            with thread_lock:
                progress_tracker[part_index] = 100


def update_progress(progress_tracker, part_index, bytes_received, total_bytes, thread_lock):
    percent_done = int((bytes_received / total_bytes) * 100)
    with thread_lock:
        progress_tracker[part_index] = percent_done


        sys.stdout.write("\r")
        sys.stdout.write(" | ".join(
            [f"Part {i + 1}: {progress_tracker.get(i, 0)}%" for i in range(4)]
        ))
        sys.stdout.flush()

def download_full_file(file_name, stop_signal):
    try:

        file_size = request_file_info(file_name)
        if file_size is None:
            return

        if file_size == 0:
            handle_empty_file(file_name)
            return


        thread_pool, partial_files = setup_download_threads(file_name, file_size, stop_signal)


        wait_for_threads_to_complete(thread_pool)

        combine_and_cleanup(file_name, partial_files)

    except Exception as e:
        print(f"Error Failed to download file: {e}")


def request_file_info(file_name):

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_sock:
            client_sock.sendto(file_name.encode(), ADDRESS)
            file_info, _ = client_sock.recvfrom(1024)

        if file_info.startswith(b"ERROR"):
            print(f"Error {file_info.decode()}")
            return None

        return int(file_info.decode())
    except Exception as e:
        print(f"Error Failed to retrieve file information: {e}")
        return None


def handle_empty_file(file_name):
    print(f"File {file_name} is empty.")
    print(f"{file_name} downloaded successfully (empty file).")


def setup_download_threads(file_name, file_size, stop_signal):

    total_chunks = (file_size + BUFFER_SIZE - 1) // BUFFER_SIZE
    thread_count = min(4, total_chunks)
    chunks_per_thread = total_chunks // thread_count
    extra_chunks = total_chunks % thread_count

    thread_pool = []
    partial_files = []
    progress_tracker = {}
    thread_lock = threading.Lock()

    bytes_left = file_size
    for index in range(thread_count):
        start, end, bytes_per_thread = calculate_chunk_range(
            index, chunks_per_thread, extra_chunks, bytes_left
        )

        part_file_path = os.path.join(OUTPUT_DIRECTORY, f"{file_name}.part{index + 1}")
        partial_files.append(part_file_path)

        thread = threading.Thread(
            target=receive_file_chunks,
            args=(ADDRESS,file_name,start,end,part_file_path,bytes_left,index,stop_signal,progress_tracker,thread_lock,),
        )

        bytes_left -= bytes_per_thread
        thread_pool.append(thread)
        thread.start()

    return thread_pool, partial_files


def calculate_chunk_range(index, chunks_per_thread, extra_chunks, bytes_left):
    start = index * chunks_per_thread
    end = start + chunks_per_thread - 1
    bytes_per_thread = min(bytes_left, chunks_per_thread * BUFFER_SIZE)

    if index == 3:
        end += extra_chunks
        bytes_per_thread = min(bytes_left, (chunks_per_thread + extra_chunks) * BUFFER_SIZE)

    return start, end, bytes_per_thread


def wait_for_threads_to_complete(thread_pool):
    for thread in thread_pool:
        thread.join()


def combine_and_cleanup(file_name, partial_files):
    final_file_path = os.path.join(OUTPUT_DIRECTORY, file_name)
    combine_parts(partial_files, final_file_path)
    print(f"\n[SUCCESS] File {file_name} combined successfully.")

    for part_file in partial_files:
        os.remove(part_file)

def client_main():
    stop_signal = threading.Event()
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        request_message = "[LIST]"
        client_socket.sendto(request_message.encode(), ADDRESS)

        available_files_data = client_socket.recv(1024).decode(ENCODING)
        if not available_files_data:
            print("Error: Server did not respond with file list.")
            return

        print("\nAvailable files:")
        available_files = available_files_data.split('\n')
        for file_name in available_files:
            print(f"  {file_name}")
        print(" ")

        available_files = [file_name.split()[0] for file_name in available_files if file_name.strip()]

        monitor_thread = threading.Thread(target=watch_input_file, daemon=True)
        monitor_thread.start()

        displayed_files = set()
        while not stop_signal.is_set():
            with thread_lock:
                files_to_process = list(files_pending)

            if files_to_process and set(files_to_process) != displayed_files:
                displayed_files = set(files_to_process)
                terminal_width = shutil.get_terminal_size().columns
                print("-" * terminal_width)
                print("\n Files waiting for download:")
                for file_name in files_to_process:
                    print(f"  {file_name}")
                print("-" * terminal_width)

            if files_to_process:
                for file_name in files_to_process:
                    with thread_lock:
                        files_pending.remove(file_name)
                        files_downloaded.add(file_name)
                    if file_name in available_files:
                        print(f"I NEED {file_name}\n")
                        download_full_file(file_name, stop_signal)
                        if not stop_signal.is_set():
                            print(f" {file_name} downloaded successfully.")
                            terminal_width = shutil.get_terminal_size().columns
                            print("-" * terminal_width)
                            print()
                        else:
                            break
                    else:
                        print(f"Error: File {file_name} not found on server.")
                        terminal_width = shutil.get_terminal_size().columns
                        print("-" * terminal_width)

    except KeyboardInterrupt:
        print("\n Download interrupted by user.")
        stop_signal.set()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client_socket.close()
        print("Client is shutting down...")

if __name__ == "__main__":
    client_main()
