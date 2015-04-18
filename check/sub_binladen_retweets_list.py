import sys
from datetime import *

from pyspark import SparkContext, SparkConf

def split_retweets(line):
    new_line = line.split("|#|")
    the_uid = []
    rt_uid = None
    rt_mid = ""
    the_time = ""
    rt_time = None
    for words in new_line :
        if words.split(":")[0] == "time" :
            the_time = words[5 :]

        if words.split(":")[0] == "mid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "rtMid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "uid" :
            uids = words.split(":")[1].split("\t")
            for i in uids :
                the_uid.append(i.split("$")[0])

        if words.split(":")[0] == "rtUid" :
            the_uid = words.split(":")[1].split("\t")[0].split("$")[0]

        if words.split(":")[0] == "rtTime" :
            rt_time = words[5 :]
    #yield (the_uid[0], (rt_mid, the_time))
    if rt_uid != None and rt_time !=None :
        if len(the_uid) > 1 :
            t1 = datetime.strptime(rt_time, "%Y-%m-%d %H:%M:%S")
            t2 = datetime.strptime(the_time, "%Y-%m-%d %H:%M:%S")
            dt = (t2-t1)/len(the_uid)
            for i in range(0, len(the_uid)-1) :
                tmp_time = datetime.strftime((t2-dt*i), "%Y-%m-%d %H:%M:%S")
                tmp_str = "%s->%s->%s" % (the_uid[i], rt_mid, the_uid[i-1])
                yield (tmp_str, tmp_time)


def split_func(line):
    new_line = line.split("|#|")
    checked = False
    for words in new_line :
        if words.split(":")[0] == "time" :
            the_time = words.split(":")[1].split()[0]
            month = the_time.split("-")[1]
            day = the_time.split("-")[2]
            if month == "05" and int(day) >= 2 :
              checked = True
        if words.split(":")[0] == "rtUid" :
          if checked :
            yield (line)
                

def split_users(line):
    new_line = line.split("'")
    yield (new_line[1], 1)
    yield (new_line[3], 1)


def split_start(word):
    if word[1][1] != None :
        yield (word[1][1])

def reduce_cunc(a, b):
    if a!=None and b!=None :
        return a+b

def reduce_time(a, b):
    if a!=None and b!=None :
        t1 = datetime.strptime(a, "%Y-%m-%d %H:%M:%S")
        t2 = datetime.strptime(b, "%Y-%m-%d %H:%M:%S")
        if t1 > t2 :
            return b
        else : 
            return a

def log_write(counts):
    f = open("log.txt", 'a')
    f.write(counts)
    f.close()



if __name__ == "__main__":
    appName = "Kechen_get_gaddafi_users"
    master = "spark://node06:7077"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/binladen_tweets"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/binladen_retweets_after"
    tweets_file = sc.textFile(tweets_path)
    rdd_tweets = tweets_file.flatMap(lambda line: split_func(line))

    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    stop_rdd = rdd_tweets.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
