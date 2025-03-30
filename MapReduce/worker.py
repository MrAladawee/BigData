import socket
import threading
import json
import sys
import os

def get_data_filename(port):
    return f"worker_data_{port}.json"

def get_p2_filename(port):
    return f"p2_worker_data_{port}.json"

def get_p3_filename(port):
    return f"p3_worker_data_{port}.json"

def save_data(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f)

def load_data(filename):
    if not os.path.exists(filename):
        return []
    with open(filename, "r") as f:
        return json.load(f)

def handle_client(conn, addr, port):
    try:
        data_received = ""
        while "\n" not in data_received:
            chunk = conn.recv(4096).decode('utf-8')
            if not chunk:
                break
            data_received += chunk

        if not data_received:
            return

        command = json.loads(data_received.strip())
        cmd = command.get("command")

        if cmd == "store_data":
            d = command.get("data", [])
            save_data(d, get_data_filename(port))
            response = {"status": "Data stored", "records": len(d)}

        elif cmd == "map_task":
            stored = load_data(get_data_filename(port))
            total = 0
            count = 0
            for record in stored:
                if "value" in record:
                    total += record["value"]
                    count += 1
            response = {"status": "Map task executed", "map_result": {"sum": total, "count": count}}

        elif cmd == "store_data_p2":
            d = command.get("data", [])
            save_data(d, get_p2_filename(port))
            response = {"status": "Data stored for p2", "records": len(d)}

        elif cmd == "p2_map":
            stored = load_data(get_p2_filename(port))
            if stored:
                local_sum = sum(stored)
                count = len(stored)
                local_mean = local_sum / count
            else:
                local_sum = 0
                count = 0
                local_mean = 0
            response = {"status": "p2 map task executed", "map_result": {"local_mean": local_mean, "count": count}}

        elif cmd == "store_data_p3":
            d = command.get("data", [])
            save_data(d, get_p3_filename(port))
            response = {"status": "Data stored for p3", "records": len(d)}

        elif cmd == "p3_map":
            stored = load_data(get_p3_filename(port))
            if not stored:
                response = {"error": "No data available for p3_map"}
                conn.sendall(json.dumps(response).encode('utf-8'))
                return

            min_value = min(stored)
            max_value = max(stored)

            bins = [(i, i + 5) for i in range(int(min_value) // 5 * 5, int(max_value) // 5 * 5 + 1, 5)]
            local_hist = {f"{b[0]}-{b[1]}": 0 for b in bins}

            for x in stored:
                for b in bins:
                    if b[0] <= x < b[1]:
                        local_hist[f"{b[0]}-{b[1]}"] += 1
                        break

            response = {"status": "p3 map task executed", "map_result": local_hist}

        else:
            response = {"error": "Unknown command"}

        message = json.dumps(response) + "\n"
        conn.sendall(message.encode('utf-8'))

    except Exception as e:
        error_msg = json.dumps({"error": str(e)}) + "\n"
        conn.sendall(error_msg.encode('utf-8'))
    finally:
        conn.close()

def start_server(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', port))
    s.listen(5)
    print(f"Worker Node запущен на порту {port}")

    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr, port))
        t.daemon = True
        t.start()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python worker.py <port>")
        sys.exit(1)
    port = int(sys.argv[1])
    start_server(port)
