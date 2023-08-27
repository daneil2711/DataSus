from hdfs import InsecureClient
import os
import sys
import platform
from multiprocessing import Pool
# Configuração do client HDFS
hdfs_host = 'http://localhost:9870'  # Altere para o host e porta do seu HDFS
hdfs_user = 'Daniel'  # Altere para o seu nome de usuário no HDFS
client = InsecureClient(hdfs_host, user=hdfs_user)

#Função Move
def move_files_to_hdfs(local_dir ,hdfs_dir):
    local_files = os.listdir(local_dir)
    
    for file in local_files:
        local_path = os.path.join(local_dir, file)
        hdfs_path = hdfs_dir + '/' + file

        client.upload(hdfs_path, local_path,overwrite=True)
        print(f"Arquivo {file} movido para o HDFS")
        
        os.remove(local_path)
        print(f"Arquivo {file} removido localmente")

#Ajuste ambiente
if platform.system() == "Windows":
    path = "C:"
else:
    path = "/mnt/c"

#Dirs chamada
local_proceeded= f"{path}/Users/Daniel/Desktop/Estudos/DataSus/rd/dbc/proceeded"
local_csv= f"{path}/Users/Daniel/Desktop/Estudos/DataSus/rd/csv"
hdfs_proceeded = '/DataSus/rd/dbc/proceeded'
hdfs_csv = '/DataSus/rd/csv'
params = [(local_proceeded,hdfs_proceeded),(local_csv,hdfs_csv)]

move_files_to_hdfs(local_proceeded,hdfs_proceeded)
move_files_to_hdfs(local_csv,hdfs_csv)