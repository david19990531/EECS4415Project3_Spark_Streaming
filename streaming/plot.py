
from flask import Flask, jsonify, request, render_template
from matplotlib.axis import XAxis, YAxis
from redis import Redis
import matplotlib.pyplot as plt
import json
import numpy as np
from datetime import datetime

app = Flask(__name__)
# @app.route('/', methods=['GET'])
# def home():
#     return "<h1> Total number of the collected repositories"
@app.route('/updateData', methods=['POST'])
def updateData():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/', methods=['GET'])
def index():
    r = Redis(host='redis', port=6379)
    data = r.get('data')
    try:
        data = json.loads(data)
    except TypeError:
        return "waiting for data..."
    try:
        pythonindex = data['LanguageName'].index('Python')
        pycountrepo = data['countrepository'][pythonindex]
        pypush = data['countoneminutepush'][pythonindex]
        pystar = data['AverageStar'][pythonindex]
        wordlist = data['Toptenwords']
    except ValueError:
        pycountrepo = 0
        pypush = 0
        pystar = 0
    try:
        javaindex = data['LanguageName'].index('Java')
        javacountrepo = data['countrepository'][javaindex]
        javapush = data['countoneminutepush'][javaindex]
        javastar = data['AverageStar'][javaindex]
    except ValueError:
        javacountrepo = 0
        javapush = 0
        javastar = 0
    try:
        goindex = data['LanguageName'].index('Go')
        gocountrepo = data['countrepository'][goindex]
        gopush = data['countoneminutepush'][goindex]
        gostar = data['AverageStar'][goindex]
    except ValueError:
        gocountrepo = 0
        gopush = 0
        gostar = 0
        
    try:
        golist = wordlist[0]
        javalist = wordlist[1]
        pythonlist = wordlist[2]
        newgolist = []
        newjavalist = []
        newpythonlist = []
        for item in golist:
            newitem = (item[0], item[1])
            newgolist.append(newitem)
        for item in javalist:
            newitem = (item[0], item[1])
            newjavalist.append(newitem)
        for item in pythonlist:
            newitem = (item[0], item[1])
            newpythonlist.append(newitem)
        
        pythont_a = newpythonlist[0]
        pythont_b = newpythonlist[1]
        pythont_c = newpythonlist[2]
        pythont_d = newpythonlist[3]
        pythont_e = newpythonlist[4]
        pythont_f = newpythonlist[5]
        pythont_g = newpythonlist[6]
        pythont_h = newpythonlist[7]
        pythont_i = newpythonlist[8]
        pythont_j = newpythonlist[9]
        java_a = newjavalist[0]
        java_b = newjavalist[1]
        java_c = newjavalist[2]
        java_d = newjavalist[3]
        java_e = newjavalist[4]
        java_f = newjavalist[5]
        java_g = newjavalist[6]
        java_h = newjavalist[7]
        java_i = newjavalist[8]
        java_j = newjavalist[9]
        go_a = newgolist[0]
        go_b = newgolist[1]
        go_c = newgolist[2]
        go_d = newgolist[3]
        go_e = newgolist[4]
        go_f = newgolist[5]
        go_g = newgolist[6]
        go_h = newgolist[7]
        go_i = newgolist[8]
        go_j = newgolist[9]

    except ValueError:
        pass
    now = datetime.now()
    rigntnowtime = now.strftime("%H:%M:%S")
    rightnowtimelist.append(rigntnowtime)
    pythonpushlist.append(pypush)
    javapushlist.append(javapush)
    gopushlist.append(gopush)
    plt.figure(1)
    # y = [pythonpushlist, javapushlist, gopushlist]
    # labels = ["Python", "Java", "Go"]
    # for y_arr, label in zip(y, labels):
    #     plt.plot(rightnowtimelist, y_arr, label=label)
    plt.plot(rightnowtimelist, pythonpushlist, color = "green", label = "Python")
    plt.plot(rightnowtimelist, javapushlist, color = "red", label = "Java")
    plt.plot(rightnowtimelist, gopushlist, color = "yellow", label = "Go")
    plt.legend()
    plt.title('Number of the collected repositoreis with changes pushed during the last 60 seconds')
    plt.xlabel('Time')
    plt.ylabel('#repositories')
    plt.savefig('/streaming/static/images/sixtysecond.png')
    
    
    height = [pystar, javastar, gostar]   
    x = [1,2,3]
    tick_label = ['Python', 'Java', 'Go']
    plt.figure(2)
    plt.bar(x, height, tick_label=tick_label, width=1.0, color=['tab:orange', 'tab:blue', 'tab:red'])
    plt.ylabel('Average number of stars')
    plt.xlabel('Pl')
    plt.title('Average number of stars')
    plt.savefig('/streaming/static/images/averagestar.png')
    return render_template('index.html', sixtysecondimage ='/static/images/sixtysecond.png', starimage = '/static/images/averagestar.png', 
                           pycountrepo = pycountrepo, javacountrepo = javacountrepo, gocountrepo = gocountrepo,
                           pythont_a = pythont_a, pythont_b = pythont_b, pythont_c = pythont_c, pythont_d = pythont_d, pythont_e = pythont_e, pythont_f = pythont_f, pythont_g = pythont_g, pythont_h = pythont_h, pythont_i = pythont_i, pythont_j = pythont_j,
                           java_a = java_a, java_b = java_b, java_c = java_c, java_d = java_d, java_e = java_e, java_f = java_f, java_g = java_g, java_h = java_h, java_i = java_i, java_j = java_j,
                           go_a = go_a, go_b = go_b, go_c = go_c, go_d = go_d, go_e = go_e, go_f = go_f, go_g = go_g, go_h = go_h, go_i = go_i, go_j = go_j)

if __name__ == '__main__':
    pythonpushlist = []
    javapushlist = []
    gopushlist = []
    rightnowtimelist = []
    app.debug = True
    app.run(host='0.0.0.0')
