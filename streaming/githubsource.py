import sys
import socket
import random
import time
import requests
import os
import json

#get token
token = os.getenv('TOKEN')
newtoken = "token " + token
datakeep = ("full_name", "pushed_at", "stargazers_count", "description")
pythonnamelist = []
javanamelist = []
gonamelist = []

#get python data
def get_python_data():
    url_python_python = 'https://api.github.com/search/repositories?q=+language:Python&sort=updated&order=desc&per_page=50'
    res_python = requests.get(url_python_python, headers={"Authorization": newtoken})
    result_json_python = res_python.json()
    return result_json_python
#get java data
def get_java_data():
    url_java_python = 'https://api.github.com/search/repositories?q=+language:Java&sort=updated&order=desc&per_page=50'
    res_java = requests.get(url_java_python, headers={"Authorization": newtoken})
    result_json_java = res_java.json()
    return result_json_java
#get go data
def get_go_data():
    url_go_python = 'https://api.github.com/search/repositories?q=+language:Go&sort=updated&order=desc&per_page=50'
    res_go = requests.get(url_go_python, headers={"Authorization": newtoken})
    result_json_go = res_go.json()
    return result_json_go

#delete useless attribute1
def cleandata(data):
    dict_filter = lambda x, y: dict([(i,x[i]) for i in x if i in set(y)])
    newdict = dict_filter(data, datakeep)
    return newdict
#delete useless attribute2
def cleanjsondata(data, languagetype):
    data.pop('total_count')
    data.pop('incomplete_results')
    dict_json = json.dumps(data)
    json_data = json.loads(dict_json)
    items = json_data['items']
    newitems = []
    for index in range(len(items)):
        repodata = cleandata(items[index])
        if repodata['full_name'] not in pythonnamelist:
            pythonnamelist.append(repodata['full_name'])
            newitems.append(repodata)
        else:
            continue
    json_data['items'] = newitems
    json_data[languagetype] = json_data.pop('items')
    spark_data = json.dumps(json_data)
    return spark_data

#Use Socket to send data
TCP_IP = "0.0.0.0"  # returns local IP
TCP_PORT = 9998
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting sending data.")

while True:
    try:
        python_result = get_python_data()
        java_result = get_java_data()
        go_result = get_go_data()
        spark_python = cleanjsondata(python_result, "Python")
        spark_java = cleanjsondata(java_result, "Java")
        spark_go = cleanjsondata(go_result, "Go")
        conn.send(f"{spark_python}\n".encode())
        conn.send(f"{spark_java}\n".encode())
        conn.send(f"{spark_go}\n".encode())
        print(spark_python)
        print(spark_java)
        print(spark_go)
        time.sleep(15)
    except KeyboardInterrupt:
        s.shutdown(socket.SHUT_RD)
