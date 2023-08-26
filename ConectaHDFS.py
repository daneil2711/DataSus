from urllib.request import urlopen
from hdfs import InsecureClient

hdfs_host = 'http://localhost:9870'  # Altere para o host e porta do seu HDFS
hdfs_user = 'Daniel'  # Altere para o seu nome de usuário no HDFS
client = InsecureClient(hdfs_host, user=hdfs_user)

ftp_address = 'ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RDAC2201.dbc'
hdfs_path = '/test/RDAC2201.dbc'
with urlopen(ftp_address) as response:
    content = response.read()
    # You can also use append=True
    # Further reference: https://hdfscli.readthedocs.io/en/latest/api.html#hdfs.client.Client.write
    with client.write(hdfs_path,overwrite=True) as writer:
        writer.write(content)