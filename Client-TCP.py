import socket
import threading
import os
import time
import sys


# Cấu hình khách
HOST = input("Enter HOST IP: ")
PORT = int(input("Port: "))
ADDR = (HOST, PORT)
FORMAT = 'utf-8'
FILE_LIST_PATH = os.path.dirname(__file__)  
OUTPUT_PATH = os.path.join(FILE_LIST_PATH, "output_folder")

lock = threading.Lock()
file_downloading_queue = []
file_downloaded = []
unit = 4096

if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)


def download_chunk(filename, offset, length, part_num, stop_event, progress_dict, lock):
    try:
        if stop_event.is_set():
            return  

        temp_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_conn.connect(ADDR)
        temp_conn.recv(1024).decode(FORMAT)

        request = f"{filename} {offset} {length}"
        temp_conn.send(request.encode(FORMAT))

        received = 0
        with open(f"{filename}.part{part_num}", "wb") as f:
            while received < length:
                if stop_event.is_set():
                    break
                chunk = temp_conn.recv(min(unit, length - received))
                if not chunk:
                    break
                f.write(chunk)
                received += len(chunk)

                # Tính phần trăm tiến trình và cập nhật vào dict
                percent_complete = int((received / length) * 100)
                with lock:
                    progress_dict[part_num] = percent_complete

                # Hiển thị tiến trình của tất cả các part
                with lock:
                    sys.stdout.write("\rDownloading ")
                    sys.stdout.write(" | ".join(
                        [f"Part {i}: {progress_dict.get(i, 0)}%" for i in range(1, 5)]
                    ))
                    sys.stdout.flush()
        if received != length:
            print(f"\nPart {part_num} incomplete! {received}/{length} downloaded")
        else:
            with lock:
                progress_dict[part_num] = 100  # Đảm bảo hoàn tất hiển thị

    except Exception as e:
        print(f"\nError downloading part {part_num} of {filename}: {e}")
    finally:
        temp_conn.close()



def merge_file(filename, num_parts):
    try:
        file_path = os.path.join(OUTPUT_PATH, filename)
        total_size = 0
        completed_size = 0

        # Tính tổng kích thước tất cả các phần
        for i in range(1, num_parts + 1):
            part_filename = f"{filename}.part{i}"
            if not os.path.exists(part_filename):
                print(f"\n{part_filename} is missing. Skip merging.")
                return
            total_size += os.path.getsize(part_filename)

        with open(file_path, "wb") as outfile:
            for i in range(1, num_parts + 1):
                part_filename = f"{filename}.part{i}"
                if not os.path.exists(part_filename):
                    print(f"\n{part_filename}. Skip merging.")
                    return
                with open(part_filename, "rb") as infile:
                    while chunk := infile.read(1024 * 1024):  # Đọc từng chunk 1MB
                        outfile.write(chunk)
                        completed_size += len(chunk)

                        # Hiển thị tiến trình hợp nhất
                        percent_complete = int((completed_size / total_size) * 100)
                        sys.stdout.write(f"\rMerging {filename}: {percent_complete}% complete")
                        sys.stdout.flush()

                os.remove(part_filename)

        print(f"\n-->File {filename} merged successfully.")

    except Exception as e:
        print(f"\nError merging file {filename}: {e}")


def remove_parts(filename, num_parts):
    # Xóa các phần của file
    for i in range(1, num_parts + 1):
        part_filename = f"{filename}.part{i}"
        if os.path.exists(part_filename):
            os.remove(part_filename)
            print(f"Removed {part_filename}")

def monitor_input_file():
    input_path = os.path.join(FILE_LIST_PATH, "input.txt")
    while True:
        if os.path.exists(input_path):
            with open(input_path, 'r') as file:
                files_to_download = set(line.strip() for line in file if line.strip())
            with lock:
                # thêm file mới vào hàng đợi
                for file in files_to_download:
                    if file not in file_downloading_queue and file not in file_downloaded:
                        file_downloading_queue.append(file)
        time.sleep(5)

def client_program():
    stop_event = threading.Event()
    progress_dict = {}  # theo dõi tiến độ
    lock = threading.Lock()  # Khóa đảm bảo tính ổn định trong môi trường đa luồng
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)
        print(f"***Client connected to {HOST}:{PORT}***")

        # Nhận và hiển thị danh sách file Server có
        file_list = client.recv(1024).decode(FORMAT)
        if not file_list:
            print("No responses")
            return

        print("\nAvailable files:")
        available_files = file_list.split('\n')
        for filename in available_files:
            print(f"  {filename}")
        print(" ")

        available_files = [filename.split()[0] for filename in available_files if filename.strip()]

        monitor_thread = threading.Thread(target=monitor_input_file, daemon=True)
        monitor_thread.start()
        printed_files = []

        while True:
            with lock:
                files_to_download = file_downloading_queue.copy()

            if files_to_download and set(files_to_download) != printed_files:
                printed_files = files_to_download.copy()

                print("\nFile waiting for download:")
                for filename in files_to_download:
                    print(f"  {filename}")
                if not files_to_download:
                    print(" None")
                print("*"*100)

                for filename in files_to_download:
                    file_downloading_queue.remove(filename)
                    file_downloaded.append(filename)
                    if filename in available_files:
                        print(f"I need {filename}, give me, please.\n")
                        client.send(filename.encode(FORMAT))
                        response = client.recv(1024).decode(FORMAT)

                        if "Error" in response:
                            print(response)
                            continue

                        file_size = int(response)

                        if file_size == 0:
                            print(f"File {filename} is empty.")
                            print(f"{filename} downloaded successfully (empty file).")
                            continue

                        num_parts = min(4, file_size)  # giới hạn 4 phần
                        part_size = file_size // num_parts
                        threads = []

                        for i in range(1, num_parts + 1):
                            offset = (i - 1) * part_size
                            length = part_size if i < num_parts else file_size - offset
                            thread = threading.Thread(
                                target=download_chunk,
                                args=(filename, offset, length, i, stop_event, progress_dict, lock)
                            )
                            threads.append(thread)
                            thread.start()

                        while any(thread.is_alive() for thread in threads):
                            with lock:
                                sys.stdout.write("\r")
                                sys.stdout.write(" | ".join(
                                    [f"Part {i}: {progress_dict.get(i, 0)}%" for i in range(1, num_parts + 1)]
                                ))
                                sys.stdout.flush()
                            time.sleep(0.1)

                        for thread in threads:
                            thread.join()

                        if not stop_event.is_set():
                            sys.stdout.write("\n")
                            merge_file(filename, num_parts)
                            print(f"\nI received {filename}. Thanks.")
                            print("-"*100)
                        else:
                            remove_parts(filename, num_parts)
                            print("Cleanup completed.")
                            break

                    else:
                        print(f"Error: File {filename} not found.")

    except KeyboardInterrupt:
        print("\nProcess is interrupted")
        stop_event.set()  # tín hiệu kết thúc đa luồng

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()
        print("Client is shutting down...")

if __name__ == "__main__":
    client_program()