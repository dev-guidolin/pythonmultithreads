import time
import sqlite3
import concurrent.futures
import sys

if len(sys.argv) < 2:
    print("Forneça o caminho do arquivo como argumento.")
    sys.exit(1)

file_path = sys.argv[1]

def insert_batch(records, successful_inserts):
    connection = sqlite3.connect('database.db')
    cursor = connection.cursor()

    insert_query = 'INSERT INTO dados (col1, col2, col3) VALUES (?, ?, ?)'

    try:
        cursor.executemany(insert_query, records)
        connection.commit()
        successful_inserts.extend(records)
    except sqlite3.Error as e:
        print(f'Erro ao inserir no banco de dados: {e}')

    connection.close()


executions = 0
successful_inserts = []

start_time = time.time()

with sqlite3.connect('database.db') as connection:
    cursor = connection.cursor()
    connection.commit()

batch_size = 100000000
num_threads = 10
batch = []

try:
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if 'localhost' not in line:
                line = line.replace('https://', '').replace('http://', '').replace('www.', '')

                parts = line.split(':')
                if len(parts) == 3:
                    batch.append((parts[0], parts[1], parts[2]))

                    if len(batch) >= batch_size:
                        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                            executor.submit(insert_batch, batch, successful_inserts)
                        executions += 1
                        batch = []
except FileNotFoundError:
    print(f'Arquivo não encontrado: {file_path}')
    sys.exit(1)

if batch:
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.submit(insert_batch, batch, successful_inserts)
    executions += 1

end_time = time.time()

runtime = end_time - start_time

total_saved = len(successful_inserts)
print(f'Quantidade de Execuções: {executions}')
print(f'Tempo de Execução: {runtime} segundos')
print(f'Total de Registros Salvos: {total_saved}')
