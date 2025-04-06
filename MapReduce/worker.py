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

def get_p4_filename(port):
    return f"p4_worker_data_{port}.json"

def get_p5_filename(port):
    return f"p5_worker_data_{port}.json"

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

        elif cmd == "store_data_p2":
            d = command.get("data", [])
            save_data(d, get_p2_filename(port))
            response = {"status": "Data p2 отправлены", "records": len(d)}

        elif cmd == "p2_map":
            stored = load_data(get_p2_filename(port))
            if stored:
                local_sum = sum(stored)
                count = len(stored)
                local_mean = local_sum / count
            else:
                #local_sum = 0
                count = 0
                local_mean = 0
            response = {"status": "p2 map выполнен", "map_result": {"local_mean": local_mean, "count": count}}

        elif cmd == "store_data_p3":
            d = command.get("data", [])
            save_data(d, get_p3_filename(port))
            response = {"status": "Data p3 отправлены", "records": len(d)}


        elif cmd == "p3_map":

            stored = load_data(get_p3_filename(port))

            if not stored:
                response = {"error": "Нет данных по p3_map"}

                conn.sendall(json.dumps(response).encode('utf-8'))

                return

            # Фиксированные интервалы: от 1 до 8 (всего 8 интервалов)

            bins = [(i, i + 1) for i in range(1, 9)]

            local_hist = {f"{b[0]}-{b[1]}": 0 for b in bins}

            for x in stored:

                for b in bins:

                    if b[0] <= x < b[1]:
                        local_hist[f"{b[0]}-{b[1]}"] += 1

                        break  # нашли подходящий интервал, выходим

            response = {"status": "p3 map выполнен", "map_result": local_hist}

        elif cmd == "store_data_p4":
            d = command.get("data", [])
            save_data(d, get_p4_filename(port))
            response = {"status": "Data p4 сохранены", "records": len(d)}

        elif cmd == "p4_map":
            stored = load_data(get_p4_filename(port))
            local_map = {}

            for record in stored:
                v = record.get("v")
                s = record.get("s")
                label = "R" if s == 0 else "S"
                key = str(v)
                if key not in local_map:
                    local_map[key] = []
                local_map[key].append(label)

            response = {"status": "p4 map выполнен", "map_result": local_map}

        elif cmd == "store_data_p5":
            d = command.get("data", [])
            save_data(d, get_p5_filename(port))
            response = {"status": "Data p5 сохранены", "records": len(d)}

        elif cmd == "p5_map":
            stored = load_data(get_p5_filename(port))
            map_result = []
            for row in stored:
                m = row["m"]
                i = row["i"]
                j = row["j"]
                v = row["v"]
                if m == 0:
                    # элемент из M0 (размер 14x7), j — от 1 до 7, k — от 1 до 9
                    for k in range(1, 10):  # k=1..9
                        map_result.append([[i, k], ['M', j, v]])
                elif m == 1:
                    # элемент из M1 (размер 7x9), i — от 1 до 14
                    for row_i in range(1, 15):  # i=1..14
                        map_result.append([[row_i, j], ['N', i, v]])

            response = {"status": "p5 map выполнен", "map_result": map_result}

        else:
            response = {"error": "Ошиба"}

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
