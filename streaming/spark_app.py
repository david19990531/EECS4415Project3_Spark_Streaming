import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import json
from datetime import datetime
from datetime import timedelta
import time
import re
import requests


datakeep = ("full_name", "pushed_at", "stargazers_count", "description")
pythonnamelist = []
javanamelist = []
gonamelist = []
# Claculate count update new count
def aggregate_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

#new star has the tuple value
#Update new average star
def aggregate_average(new_star, total_star):
    oldaveragestar = 0
    averagestar = [items[0] for items in new_star]
    numbercount = [items[1] for items in new_star]
    if total_star:
        #get values
        oldcount = total_star[1]
        oldaveragestar = total_star[0]
        new_total_counts = oldcount + numbercount[0]
        old_total_star = round(float(oldaveragestar) * int(oldcount),1)
        new_total_star = round(float(averagestar[0]) * int(numbercount[0]),1)
        total_star = old_total_star + new_total_star
        #Because need to calculate new average star, then ((oldcount*oldstar)+(newcount*newstar)) / (totalcount)
        total_average_star = round((total_star / new_total_counts),1)
        return (total_average_star, new_total_counts)
    else:
        average = averagestar[0]
        new_total_counts = numbercount[0]
        return (average, new_total_counts)

def aggregate_wordcount(new_list, total_list):
    totallist = []
    if len(new_list) > 0:
        getthelist = new_list[0]
        if total_list:
            newtotaldict = dict(total_list)
            for new_wordtuple in getthelist:
                if new_wordtuple[0] in newtotaldict.keys():
                    newtotaldict[new_wordtuple[0]] = newtotaldict[new_wordtuple[0]] + new_wordtuple[1]
                else:
                    newtotaldict[new_wordtuple[0]] = new_wordtuple[1]
            dicttolist = [(k, v) for k, v in newtotaldict.items()]
            totallist = sorted(dicttolist, key=lambda tup: tup[1], reverse= True)
            return totallist
            
        else:
            newlist = sorted(getthelist, key=lambda tup: tup[1], reverse= True)
            totallist = newlist
            return totallist
    else:
        return total_list
                    
def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']

def send_df_to_dashboard(df):
    url = 'http://webapp:5000/updateData'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)
    
def readlines(self):
    data = self.sock.recv(1024).decode()
    print(data)
    
def cleanduplicate(data, languagetype):
    if languagetype == "Python":
        data = json.loads(data)
        items = data.get("Python")
        newitems = []
        for index in range(len(items)):
            repodata = items[index]
            if repodata['full_name'] not in pythonnamelist:
                pythonnamelist.append(repodata['full_name'])
                newitems.append(repodata)
            else:
                continue
        data["Python"] = newitems
        
        print(pythonnamelist)
        spark_data = json.dumps(data)
        spark_data_noduplicate = json.loads(spark_data)
        return spark_data_noduplicate
    
#show the table and write it into data.txt        
def process_new_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    with open("data.txt", "a+") as w:
        now = datetime.now()
        starttime = now.strftime("%Y%m%d%H%M%S") 
        startime = starttime
        endtime = time + timedelta(minutes = 1)
        endtime = endtime.strftime('%Y%m%d%H%M%S')
        w.write("%s:%s\n" % (startime, endtime))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(LanguageName=w[0], countrepository =w[1][0][0][0], countoneminutepush = w[1][0][0][1], AverageStar = w[1][0][1][0], Toptenwords = w[1][1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select LanguageName, countrepository, countoneminutepush, AverageStar, Toptenwords from results order by LanguageName")
        new_results_df.show(20, False)
        send_df_to_dashboard(new_results_df)
        languagelist = new_results_df.select("LanguageName").rdd.map(lambda x: x[0]).collect()
        countlist = new_results_df.select("countrepository").rdd.map(lambda x: x[0]).collect()
        pushtimelist = new_results_df.select("countoneminutepush").rdd.map(lambda x: x[0]).collect()
        starlist = new_results_df.select("AverageStar").rdd.map(lambda x: x[0]).collect()
        wordlist = new_results_df.select("Toptenwords").rdd.map(lambda x: x[0]).collect()
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
        
        # print("%s:%s" % (startime, endtime))
        # print("%s:%s:%s" % (str(languagelist[0]), str(countlist[0]), str(starlist[0])))
        # print("%s:%s:%s" % (str(languagelist[1]), str(countlist[1]), str(starlist[1])))
        # print("%s:%s:%s" % (str(languagelist[2]), str(countlist[2]), str(starlist[2])))        
        # print("Go:" + str(newgolist))
        # print("Java:" + str(newjavalist)) 
        # print("Python:" + str(newpythonlist))

        # with open("data.txt", "a+") as w:
        #     w.write("%s:%s:%s\n" % (str(languagelist[0]), str(countlist[0]), str(starlist[0])))
        #     w.write("%s:%s:%s\n" % (str(languagelist[1]), str(countlist[1]), str(starlist[1])))
        #     w.write("%s:%s:%s\n" % (str(languagelist[2]), str(countlist[2]), str(starlist[2])))
        # datalanguagelist = [i[0] for i in languagelist]
        # datacountlist = [i[0] for i in countlist]
        # datapushtimelist = [i[0] for i in pushtimelist]
        # datastarlist = [i[0] for i in starlist]

    

    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
#compute now time and help to calculate 60second push    
def rightnowtime():
    now = datetime.now()
    #yearmonthdatehourminutesecond
    datewithtime = now.strftime("%Y%m%d%H%M%S")
    return datewithtime
# Compute push_at time and rightnow time, if nowtime - push_attime <= 60 it means count + 1        
def comparetime(data):
    #data is a list
    count = 0
    if int(len(data)) > 0:
        for itemsdict in data:
            for key in itemsdict:
                if key == 'pushed_at':
                    repopushtime = itemsdict['pushed_at']
                    repopushtime = repopushtime.replace('-', '')
                    repopushtime = repopushtime.replace(':', '')
                    repopushtime = repopushtime.replace('T', '')
                    repopushtime = repopushtime.replace('Z', '')
                    nowtime = rightnowtime()
                    FMT = '%Y%m%d%H%M%S'
                    d1 = datetime.strptime(nowtime, FMT)
                    d2 = datetime.strptime(repopushtime, FMT)
                    d1_ts = time.mktime(d1.timetuple())
                    d2_ts = time.mktime(d2.timetuple())
                    seconddifference = int(d1_ts-d2_ts)
                    if (seconddifference <= 60):
                        count = count + 1
        return count
    else:
        return 0
# Calculate total star
def totalstar(data):
    star = 0
    count = 0
    if int(len(data)) > 0:
        for itemsdict in data:
            for key in itemsdict:
                if key == 'stargazers_count':
                    star = star + int(itemsdict['stargazers_count'])
                    count = count + 1
        return (star, count)
    else:
        return (0, 0)
#Find Description in the list, Eg: [(x, 1), (y, 1)...]
def finddescription(data, languagetype):
    strlist = []
    string = ''
    if (len(data)) > 0:
        if languagetype == "Python":
            for itemsdict in data:
                if itemsdict['description'] != None:
                    newstring = re.sub('[^a-zA-Z ]', '', itemsdict['description'])
                    newlist = newstring.split()
                    for word in newlist:
                        strlist.append(word)
                else:
                    continue
            for word in strlist:
                newword = "Python!=" + word + " "
                string = string +newword
            return string 
        if languagetype == "Java":
            for itemsdict in data:
                if itemsdict['description'] != None:
                    newstring = re.sub('[^a-zA-Z ]', '', itemsdict['description'])
                    newlist = newstring.split()
                    for word in newlist:
                        strlist.append(word)
                else:
                    continue
            for word in strlist:
                newword = "Java!=" + word + " "
                string = string +newword
            return string    
        else:
            #Go
            for itemsdict in data:
                if itemsdict['description'] != None:
                    newstring = re.sub('[^a-zA-Z ]', '', itemsdict['description'])
                    newlist = newstring.split()
                    for word in newlist:
                        strlist.append(word)
                else:
                    continue
            for word in strlist:
                newword = "Go!=" + word + " "
                string = string +newword
            return string                 
    else:
        return None
def splitstring(text):
    newtext = text.split("!=") 
    return newtext       

if __name__ == "__main__":
    global null
    null = ''
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9998 
    sc = SparkContext(appName="GithubData")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 60)
    ssc.checkpoint("checkpoint_GithubData")
    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    dictlanguage = data.map(lambda items: json.loads(items))

    #count the language number
    numrepo = dictlanguage.map(lambda x: (("Python", len(x.pop(list(x.keys())[0]))) if "Python" in x else (('Java', len(x.pop(list(x.keys())[0])))if "Java" in x else ('Go', len(x.pop(list(x.keys())[0])))))).reduceByKey(lambda a,b: a+b)
    
    #count pushtime with last 60s
    
    timerepo = dictlanguage.map(lambda x:(("Python", comparetime(x.pop(list(x.keys())[0]))) if "Python" in x else (('Java', comparetime(x.pop(list(x.keys())[0])))if "Java" in x else ('Go', comparetime(x.pop(list(x.keys())[0]))))))
    timerepo = timerepo.reduceByKey(lambda a,b: a+b)
    # count the total star and avearge star
    starrepo = dictlanguage.map(lambda x:(("Python", (totalstar(x.pop(list(x.keys())[0])))) if "Python" in x else (('Java', (totalstar(x.pop(list(x.keys())[0]))))if "Java" in x else ('Go', (totalstar(x.pop(list(x.keys())[0])))))))
    starrepo = starrepo.reduceByKey(lambda a,b: (a[0] + b[0], a[1] + b[1]))
    starrepo=starrepo.map(lambda x : (x[0], (round((x[1][0]/x[1][1]), 1), x[1][1])) if x[1][1] != 0 else (x[0], (round(0, 1), x[1][1])))
    
    # count the top 10 words
    # Eg: ("Python!=x Python!=y Python!=x Python!=z ...")
    wordrepo = dictlanguage.map(lambda x: ((finddescription(x.pop(list(x.keys())[0]), "Python")) if "Python" in x else ((finddescription(x.pop(list(x.keys())[0]), "Java"))if "Java" in x else (finddescription(x.pop(list(x.keys())[0]), "Go")))))
    # use flatmap to separate " "
    wordrepo = wordrepo.filter(lambda x: x is not None)
    wordrepo = wordrepo.flatMap(lambda x: x.split(" "))
    # each word with 1
    wordpair = wordrepo.map(lambda word: (word, 1))
    #plus together
    countword = wordpair.reduceByKey(lambda x, y: x + y)
    countword = countword.map(lambda x: ((x[0].split("!="), x[1])) if len(x[0]) > 0 else None)
    #remove None Value
    countword = countword.filter(lambda x: x is not None)
    countword = countword.map(lambda x: (x[0][0],(x[0][1], x[1])))
    # let value of same languagetype put them into a list
    countword = countword.groupByKey().mapValues(list)
    

    aggregated_counts = numrepo.updateStateByKey(aggregate_count)
    aggregated_timecount = timerepo.updateStateByKey(aggregate_count)
    aggregated_starcount = starrepo.updateStateByKey(aggregate_average)
    aggregatecountword = countword.updateStateByKey(aggregate_wordcount)
    aggregatecountword = aggregatecountword.map(lambda x: (x[0], x[1][0:10]))
    # Join 4 datastream
    newdstream = aggregated_counts.join(aggregated_timecount)
    newdstream = newdstream.join(aggregated_starcount)
    newdstream = newdstream.join(aggregatecountword)
    newdstream.foreachRDD(process_new_rdd)
    newdstream.pprint()
    aggregated_counts.pprint()
    aggregated_timecount.pprint()
    aggregated_starcount.pprint()
    aggregatecountword.pprint()
    
    
    ssc.start()
    ssc.awaitTermination()