import sys
from datetime import *

from pyspark import SparkContext, SparkConf

def split_retweets(line):
    new_line = line.split("|#|")
    the_uid = []
    rt_uid = None
    rt_mid = ""
    the_time = ""
    rt_time= None
    hours = 0
    for words in new_line :
        if words.split(":")[0] == "time" :
            the_time = words[5 :]
        if words.split(":")[0] == "rtTime" :
            rt_time = words[7 :]
        if words.split(":")[0] == "mid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "rtMid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "uid" :
            uids = words.split(":")[1].split("\t")
            for i in uids :
                the_uid.append(i.split("$")[0])

        if words.split(":")[0] == "rtUid" :
            rt_uid = words.split(":")[1].split("\t")[0].split("$")[0]
    if rt_time !=None :
        t1 = datetime.strptime(rt_time, "%Y-%m-%d %H:%M:%S")
        t2 = datetime.strptime(the_time, "%Y-%m-%d %H:%M:%S")
        hours = (t2-t1).total_seconds()/3600
        hours = round(hours, 2)

    if rt_uid != None and len(the_uid) == 1 :
        tmp_str = "%s->%s->%s->%s->%f->%d" % (rt_uid, rt_mid, the_uid[0], rt_time, 0, 1)
        yield (tmp_str)

    if rt_uid != None and rt_time !=None and len(the_uid) > 1 :
        tmp_hour = hours/len(the_uid)
        t1 = datetime.strptime(rt_time, "%Y-%m-%d %H:%M:%S")
        t2 = datetime.strptime(the_time, "%Y-%m-%d %H:%M:%S")
        dt = (t2-t1)/len(the_uid)
        for i in range(0, len(the_uid)-2) :
            tmp_time = datetime.strftime((t2-dt*(i+1)), "%Y-%m-%d %H:%M:%S")
            tmp_str = "%s->%s->%s->%s->%f->%d" % (the_uid[i+1], rt_mid, the_uid[i], tmp_time, tmp_hour, 1)
            yield (tmp_str)
        tmp_str = "%s->%s->%s->%s->%f->%d" % (rt_uid, rt_mid, the_uid[-1], rt_time, tmp_hour, 1)
        yield (tmp_str)
def resplit_union(words):
    the_words = words.split("->")
    uid = the_words[0]
    mid = the_words[1]
    rt_uid = the_words[2]
    tmp_str = "%s->%s->%s" % (uid, mid, rt_uid)
    yield (tmp_str, words)

def resplit_result(words):
    tmp_out = words[1]
    if tmp_out != None :
        yield (tmp_out)

def split_users(line):
    new_line = line.split("'")
    yield (new_line[1], new_line[3])

def split_check(line):
    new_line = line.split("->")
    if new_line[-1] == "1" :
        yield (new_line[0], 1)

def split_start(word):
    if word[1][1] != None :
        yield (word[1][1])

def reduce_cunc(a, b):
    if a!=None and b!=None :
        return a+b

def reduce_dif(a, b):
    if a!=None and b!=None :
        if a[-1] == "1" :
            return a
        else :
            return b

def log_write(counts):
    f = open("log.txt", 'a')
    f.write(counts)
    f.close()



if __name__ == "__main__":
    appName = "Kechen_get_gaddafi_users"
    master = "spark://node06:7077"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)

    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/tweets_tmp2"
    origin_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/retweet_dif_with_time_hours"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/retweet_dif_with_time_hours"

    tweets_file = sc.textFile(tweets_path)
    origin_file = sc.textFile(origin_path)



    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    #
    # get rt_uid -> rt_mid -> the_uid, 1
    rdd_retweets = tweets_file.flatMap(lambda line: split_retweets(line))
    rdd_counts = rdd_retweets.count()
    stop_rdd =rdd_retweets
    stop_rdd.saveAsTextFile(output_path)
    print "&&&&\n&&&&\n&&&&\n"
    print "now the real dif counts is %s" % (rdd_counts)
    if rdd_counts < 0 :
        # get rt_uid -> rt_mid -> the_uid, 0
        rdd_merged = origin_file.union(rdd_retweets)\
                .flatMap(lambda words: resplit_union(words))\
                .reduceByKey(lambda a,b: reduce_dif(a,b))\
                .flatMap(lambda words: resplit_result(words))

        # Here some test count for debug
        tmp_str = "\nthe real dif count is%s,\n and the network dif count is unknown" % \
                (rdd_counts)
        log_write(tmp_str)



        stop_rdd = rdd_merged
        stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
