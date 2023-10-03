import os
import subprocess


def list_files_in_directory(directory):
    file_list = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_list.append(os.path.join(root, file))
    return file_list


folder_path = 'clean'

file_paths = list_files_in_directory(folder_path)

for file_path in file_paths:
    print(f'Processando o arquivo: {file_path}')
    subprocess.run(['python', 'process_files.py', file_path])

print('Processamento concluído.')
