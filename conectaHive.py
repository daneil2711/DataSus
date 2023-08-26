from pyhive import hive

host_name = "localhost"
port = 10000
user = "admin"
password = "password"
database="test"

def hiveconnection(host_name, port, user,password, database):
    conn = hive.Connection(host=host_name, port=port, username=user, password=password,
                           database=database, auth='CUSTOM')
    cur = conn.cursor()
    cur.execute('drop table test.tab')
    result = cur.fetchall()

    return result

# Call above function
output = hiveconnection(host_name, port, user,password, database)
print(output)