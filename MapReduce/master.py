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

def p2_run():
    """
    Реализация Пункта 2 (выборочное среднее CSV-файла).
    """
    csv_data = pd.read_csv('p2.csv')['x_value'].tolist()

    # Используем функцию distribute_data для распределения данных по рабочим узлам
    responses = distribute_data(csv_data)
    print("Пункт 2. Данные распределены:", responses)

    # Reduce
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


def p3_run():
    csv_data = pd.read_csv('p3.csv')['x_value'].tolist()

    # Нахождение минимального и максимального значения в данных
    min_value = min(csv_data)
    max_value = max(csv_data)

    # Создаем интервалы с шагом 5
    bins = [(i, i + 5) for i in range(int(min_value) // 5 * 5, (int(max_value) // 5 + 1)* 5, 5)]

    # Используем функцию distribute_data для распределения данных по рабочим узлам
    responses = distribute_data(csv_data)
    print("Пункт 3. Данные распределены:", responses)

    # Reduce
    map_results = []
    for address in workers:
        command = {"command": "p3_map"}  # Map
        res = send_command(address, command)
        map_results.append(res.get("map_result", {}))

    # Инициализируем гистограмму по динамически вычисленным интервалам
    global_hist = {f"{b[0]}-{b[1]}": 0 for b in bins}
    total_count = 0

    # Суммируем локальные гистограммы от всех workers
    for local_hist in map_results:
        for bin_key, count in local_hist.items():
            global_hist[bin_key] += count
            total_count += count

    result = {
        "global_histogram_counts": global_hist,
        "total_count": total_count,
        "map_results": map_results
    }
    return result

def plot_mapreduce_histogram(global_hist):
    #global_hist: Словарь {интервал: количество элементов}

    bins = list(global_hist.keys())  # Метки интервалов
    counts = list(global_hist.values())  # Количества по каждому интервалу

    plt.figure(figsize=(8, 5))
    plt.bar(bins, counts, color='blue', alpha=0.7, edgecolor='black')

    plt.xlabel("Интервалы значений X")
    plt.ylabel("Количество элементов")
    plt.title("Гистограмма распределения данных")
    plt.grid(axis='y', linestyle='--', alpha=0.6)

    plt.show()

def plot_original_histogram(data):
    """
    Строит гистограмму по исходным данным, вручную создавая интервалы
    и подсчитывая их частоты.
    """
    # Читаем данные из CSV
    # Обратите внимание, что предполагается, что данные находятся в колонке 'x_value'
    x_values = data['x_value'].tolist()

    # Получаем минимальное и максимальное значения для данных
    min_value = min(x_values)
    max_value = max(x_values)

    # Создаем интервалы с шагом 5
    bins = []
    start = int(min_value) // 5 * 5  # округление вниз до ближайшего кратного 5
    end = (int(max_value) // 5 + 1) * 5  # округление вверх до ближайшего кратного 5

    for i in range(start, end, 5):
        bins.append((i, i + 5))

    # Инициализируем гистограмму для подсчета частот
    hist = {f"{b[0]}-{b[1]}": 0 for b in bins}

    # Подсчитываем количество элементов в каждом интервале
    for x in x_values:
        for b in bins:
            if b[0] <= x < b[1]:
                hist[f"{b[0]}-{b[1]}"] += 1
                break  # Если число попало в интервал, выходим из цикла

    # Визуализация
    plt.figure(figsize=(8, 5))
    plt.bar(hist.keys(), hist.values(), color='blue', alpha=0.7, edgecolor='black')

    plt.xlabel("Интервалы значений X")
    plt.ylabel("Количество элементов")
    plt.title("Гистограмма исходных данных")
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.show()

if __name__ == "__main__":
    ''' Тестовый пример
    test_data = [
        {"value": 10},
        {"value": 20},
        {"value": 30},
        {"value": 40},
        {"value": 50},
        {"value": 60}
    ]

    print("Распределяем данные по Worker Node (стандартный пример)...")
    responses = distribute_data(test_data)
    print("Ответы от Worker Node:")
    print(responses)

    print("\nЗапуск стандартного MapReduce алгоритма...")
    result = run_algorithm()
    print("Результат Reduce-задачи:")
    print(result)
    '''

    print("\nЗапуск задачи Пункт 2: выборочное среднее...")
    p2_result = p2_run()
    print("Результат задачи Пункт 2:")
    print(p2_result)

    print("\nЗапуск задачи Пункт 3: гистограмма...")
    p3_result = p3_run()
    print("Результат задачи Пункт 3:")
    print(p3_result)
    # Используем 'global_histogram_counts' для построения гистограммы из собранных результатов
    plot_mapreduce_histogram(p3_result["global_histogram_counts"])

    # Сверка с оригиналом
    data = pd.read_csv('p3.csv')
    plot_original_histogram(data)
