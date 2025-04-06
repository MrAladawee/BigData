import socket
import json
import pandas as pd
import matplotlib.pyplot as plt

# Список адресов Worker Node (localhost)
workers = [
    ('127.0.0.1', 5000),
    ('127.0.0.1', 5001),
    ('127.0.0.1', 5002)
]

def send_command(address, command_dict):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(address)
    message = json.dumps(command_dict) + "\n"
    s.sendall(message.encode('utf-8'))

    data = ""
    while "\n" not in data:
        part = s.recv(4096).decode('utf-8')
        if not part:
            break
        data += part
    s.close()
    return json.loads(data.strip())


def distribute_data(data):
    num_workers = len(workers)
    chunk_size = len(data) // num_workers
    chunks = []

    for i in range(num_workers):
        # Последнему узлу отдаем остаток данных
        if i == num_workers - 1:
            chunk = data[i * chunk_size:]
        else:
            chunk = data[i * chunk_size:(i + 1) * chunk_size]
        chunks.append(chunk)

    responses = []
    for i, address in enumerate(workers):
        command = {"command": "store_data", "data": chunks[i]}
        res = send_command(address, command)
        responses.append(res)
    return responses

def send_p2_data_to_workers(data):
    """
    Отправляет данные для задачи Пункт 2 (выборочное среднее) на воркеры.
    """
    num_workers = len(workers)
    chunk_size = len(data) // num_workers
    chunks = []

    for i in range(num_workers):
        if i == num_workers - 1:
            chunk = data[i * chunk_size:]
        else:
            chunk = data[i * chunk_size:(i + 1) * chunk_size]
        chunks.append(chunk)

    responses = []
    for i, address in enumerate(workers):
        command = {"command": "store_data_p2", "data": chunks[i]}
        res = send_command(address, command)
        responses.append(res)
    return responses

def p2_run():
    """
    Выполняет MapReduce задачу по вычислению среднего значения.
    Предполагается, что данные уже были загружены через send_p2_data_to_workers().
    """
    map_results = []
    for address in workers:
        command = {"command": "p2_map"}  # Map
        res = send_command(address, command)
        map_results.append(res.get("map_result", {}))

    total_sum = 0
    total_count = 0
    for r in map_results:
        local_mean = r.get("local_mean", 0)
        count = r.get("count", 0)
        total_sum += local_mean * count
        total_count += count
    global_mean = total_sum / total_count if total_count > 0 else None

    result = {
        "total_sum": total_sum,
        "total_count": total_count,
        "global_mean": global_mean,
        "map_results": map_results
    }
    return result

def send_p3_data_to_workers(data):
    """
    Отправляет данные для задачи Пункт 3 (гистограмма) на воркеры.
    """
    num_workers = len(workers)
    chunk_size = len(data) // num_workers
    chunks = []

    for i in range(num_workers):
        if i == num_workers - 1:
            chunk = data[i * chunk_size:]
        else:
            chunk = data[i * chunk_size:(i + 1) * chunk_size]
        chunks.append(chunk)

    responses = []
    for i, address in enumerate(workers):
        command = {"command": "store_data_p3", "data": chunks[i]}
        res = send_command(address, command)
        responses.append(res)
    return responses

def p3_run():
    """
    Выполняет MapReduce задачу по построению гистограммы.
    Предполагается, что данные уже были загружены через send_p3_data_to_workers().
    """
    # Фиксированные интервалы 1–8: [1,2), [2,3), ..., [8,9)
    bins = [(i, i + 1) for i in range(1, 9)]

    map_results = []
    for address in workers:
        command = {"command": "p3_map"}
        res = send_command(address, command)
        map_results.append(res.get("map_result", {}))

    global_hist = {f"{b[0]}-{b[1]}": 0 for b in bins}
    total_count = 0

    for local_hist in map_results:
        for bin_key, count in local_hist.items():
            if bin_key in global_hist:
                global_hist[bin_key] += count
                total_count += count

    result = {
        "global_histogram_counts": global_hist,
        "total_count": total_count,
        "map_results": map_results
    }
    return result


def send_p4_data_to_workers(p4_data):
    """
    Распределяет данные из p4.csv между воркерами.
    Каждая запись — словарь {"s": 0 или 1, "v": значение}
    """
    num_workers = len(workers)
    chunk_size = len(p4_data) // num_workers
    chunks = []

    for i in range(num_workers):
        if i == num_workers - 1:
            chunk = p4_data[i * chunk_size:]
        else:
            chunk = p4_data[i * chunk_size:(i + 1) * chunk_size]
        chunks.append(chunk)

    responses = []
    for i, address in enumerate(workers):
        command = {"command": "store_data_p4", "data": chunks[i]}
        res = send_command(address, command)
        responses.append(res)
    return responses


def p4_run():
    """
    Запускает MapReduce-задачу по вычислению разности множеств S0 - S1.
    """
    map_results = []
    for address in workers:
        command = {"command": "p4_map"}
        res = send_command(address, command)
        map_results.append(res.get("map_result", {}))

    combined = {}
    for local_map in map_results:
        for v_str, source_list in local_map.items():
            if v_str not in combined:
                combined[v_str] = []
            combined[v_str].extend(source_list)

    result_diff = [int(v) for v, sources in combined.items() if sources == ["R"]]

    return {
        "difference": sorted(result_diff),
        "intermediate": combined
    }

def plot_mapreduce_histogram(global_hist):
    """
    Визуализирует гистограмму, собранную с помощью MapReduce.
    global_hist: словарь {интервал: количество элементов}, например {'1-2': 5, ..., '8-9': 3}
    """

    # Сортируем ключи по порядку чисел
    sorted_bins = sorted(global_hist.keys(), key=lambda x: int(x.split('-')[0]))
    counts = [global_hist[b] for b in sorted_bins]

    plt.figure(figsize=(8, 5))
    plt.bar(sorted_bins, counts, color='blue', alpha=0.7, edgecolor='black')

    plt.xlabel("Интервалы значений X (1-8)")
    plt.ylabel("Количество элементов")
    plt.title("MapReduce: Гистограмма распределения от 1 до 8")
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_original_histogram(data):
    """
    Строит гистограмму по исходным данным, используя фиксированные интервалы от 1 до 8.
    """
    x_values = data['x_value'].tolist()

    # Интервалы: [1,2), [2,3), ..., [8,9)
    bins = [(i, i + 1) for i in range(1, 9)]
    hist = {f"{b[0]}-{b[1]}": 0 for b in bins}

    for x in x_values:
        for b in bins:
            if b[0] <= x < b[1]:
                hist[f"{b[0]}-{b[1]}"] += 1
                break

    sorted_bins = sorted(hist.keys(), key=lambda x: int(x.split('-')[0]))
    counts = [hist[b] for b in sorted_bins]

    plt.figure(figsize=(8, 5))
    plt.bar(sorted_bins, counts, color='green', alpha=0.7, edgecolor='black')

    plt.xlabel("Интервалы значений X (1-8)")
    plt.ylabel("Количество элементов")
    plt.title("Исходная гистограмма данных от 1 до 8")
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def send_p5_data_to_workers(p5_data):
    num_workers = len(workers)
    chunk_size = len(p5_data) // num_workers
    chunks = []

    for i in range(num_workers):
        if i == num_workers - 1:
            chunk = p5_data[i * chunk_size:]
        else:
            chunk = p5_data[i * chunk_size:(i + 1) * chunk_size]
        chunks.append(chunk)

    responses = []
    for i, address in enumerate(workers):
        command = {"command": "store_data_p5", "data": chunks[i]}
        res = send_command(address, command)
        responses.append(res)
    return responses


def p5_run():
    """
    MapReduce для перемножения матриц.
    """
    map_results = []
    for address in workers:
        command = {"command": "p5_map"}
        res = send_command(address, command)
        map_results.extend(res.get("map_result", []))  # Список пар (key, value)

    # Сгруппировать по ключу (i, k)
    grouped = {}
    for item in map_results:
        key = tuple(item[0])  # (i, k)
        val = tuple(item[1])  # ('M'/'N', j, value)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(val)

    result_matrix = {}

    for (i, k), values in grouped.items():
        M_values = {}
        N_values = {}

        for origin, j, v in values:
            if origin == 'M':
                M_values[j] = v
            elif origin == 'N':
                N_values[j] = v

        sum_prod = 0
        for j in set(M_values.keys()) & set(N_values.keys()):
            sum_prod += M_values[j] * N_values[j]

        result_matrix[(i, k)] = sum_prod

    return result_matrix

def print_pretty_matrix(matrix, rows=14, cols=9):
    print("\nРезультирующая матрица P (14x9):\n")

    # Заголовок
    header = ["     "] + [f"{j:>8}" for j in range(1, cols + 1)]
    print("".join(header))
    print("     " + "--------" * cols)

    for i in range(1, rows + 1):
        row_vals = []
        for j in range(1, cols + 1):
            val = matrix.get((i, j), 0)
            row_vals.append(f"{val:8.2f}")
        print(f"{i:>3} |" + "".join(row_vals))

def print_simple_matrix(matrix, rows=14, cols=9):
    print("\nМатрица P:")
    for i in range(1, rows + 1):
        row = []
        for j in range(1, cols + 1):
            val = matrix.get((i, j), 0)
            row.append(f"{val:.2f}")
        print("\t".join(row))

if __name__ == "__main__":
    print("\nПодготовка: Отправка данных для Пункт 2...")
    p2_data = pd.read_csv('p2.csv')['x_value'].tolist()
    p2_send_result = send_p2_data_to_workers(p2_data)
    print("Данные P2 отправлены:", p2_send_result)

    print("\nЗапуск задачи Пункт 2: выборочное среднее...")
    p2_result = p2_run()
    print("Результат задачи Пункт 2:")
    print(p2_result)

    print("\nПодготовка: Отправка данных для Пункт 3...")
    p3_data = pd.read_csv('p3.csv')['x_value'].tolist()
    p3_send_result = send_p3_data_to_workers(p3_data)
    print("Данные P3 отправлены:", p3_send_result)

    print("\nЗапуск задачи Пункт 3: гистограмма...")
    p3_result = p3_run()
    print("Результат задачи Пункт 3:")
    print(p3_result)

    # Построение MapReduce гистограммы
    plot_mapreduce_histogram(p3_result["global_histogram_counts"])

    # Сравнение с оригиналом
    plot_original_histogram(pd.read_csv('p3.csv'))

    print("\nПодготовка: Отправка данных для Пункт 4...")
    p4_df = pd.read_csv('p4.csv')
    p4_data = p4_df.to_dict(orient='records')
    p4_send_result = send_p4_data_to_workers(p4_data)
    print("Данные P4 отправлены:", p4_send_result)

    print("\nЗапуск задачи Пункт 4: разность множеств S0 - S1...")
    p4_result = p4_run()
    print("Результат задачи Пункт 4 (разность S0 - S1):")
    print(p4_result["difference"])

    print("\nПодготовка: Отправка данных для Пункт 5...")
    p5_df = pd.read_csv('p5.csv')
    p5_data = p5_df.to_dict(orient='records')
    p5_send_result = send_p5_data_to_workers(p5_data)
    print("Данные P5 отправлены:", p5_send_result)

    print("\nЗапуск задачи Пункт 5: умножение матриц...")
    p5_result = p5_run()
    print("Результат перемножения матриц (непустые элементы):")
    for key in sorted(p5_result.keys()):
        print(f"P{key} = {p5_result[key]}")
    print_simple_matrix(p5_result)
