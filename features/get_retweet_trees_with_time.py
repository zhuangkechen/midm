import sys
from datetime import *

from pyspark import SparkContext, SparkConf

def split_uid_mid(line):
    new_line = line.split("|#|")
    the_uid = ""
    rt_uid = None
    rt_mid = ""
    the_time = ""
    for words in new_line :
        if words.split(":")[0] == "time" :
            the_time = words[5 :]

        if words.split(":")[0] == "mid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "rtMid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "uid" :
            the_uid = words.split(":")[1].split("\t")[0].split("$")[0]

    yield (the_uid, (rt_mid, the_time))

def split_retweets(line):
    new_line = line.split("|#|")
    the_uid = ""
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
            the_uid = words.split(":")[1].split("\t")[0].split("$")[0]

        if words.split(":")[0] == "rtUid" :
            rt_uid = words.split(":")[1].split("\t")[0].split("$")[0]
    if rt_time !=None :
        t1 = datetime.strptime(rt_time, "%Y-%m-%d %H:%M:%S")
        t2 = datetime.strptime(the_time, "%Y-%m-%d %H:%M:%S")
        hours = (t2-t1).total_seconds()/3600
        hours = round(hours, 2)

    if rt_uid != None :
        yield (rt_uid, (rt_mid, the_uid, hours))

def split_dif_0(words, dif) :
    the_uid = words[0]
    rt_mid = words[1][0][0]
    the_time = words[1][0][1]
    rt_uid = words[1][1]
    if rt_mid != None and rt_uid != None and the_time != None :
        tmp_str = "%s->%s->%s" % (the_uid, rt_mid, rt_uid)
        yield (tmp_str, the_time)

def split_dif(words, dif) :
    the_uid = words[0]
    rt_mid = words[1][0]
    rt_uid = words[1][1]
    hours = words[1][2]
    if rt_mid != None and rt_uid != None and hours != None :
        tmp_str = "%s->%s->%s" % (the_uid, rt_mid, rt_uid)
        yield (tmp_str, (hours, dif))

def split_tweet_dif(words) :
    total_hours = (30+31)*24
    msg = words[0].split("->")
    the_uid = msg[0]
    rt_mid = msg[1]
    rt_uid = msg[2]
    the_time = words[1][0]
    if words[1][1] == None :
        hours = None
        def_1 = None
    else :
        hours = words[1][1][0]
        def_1 = words[1][1][1]
    dif = 0
    if def_1 == None :
        dif = 0
    else :
        dif = 1
    if hours == None :
        hours = 0
    else :
        hours = round(float(hours)/total_hours, 5)
    tmp_str  = "%s->%s->%s->%s->%f->%d" % (the_uid, rt_mid, rt_uid, the_time, hours, dif)
    yield (tmp_str)


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
    if a!=0 and b!=0 :
        return a+b

def log_write(counts):
    f = open("log.txt", 'a')
    f.write(counts)
    f.close()



if __name__ == "__main__":
    appName = "Kechen_get_gaddafi_users"
    master = "spark://node06:7077"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)

    network_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_tmp2"
    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/tweets_tmp2"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/retweet_dif_with_time_hours"

    tweets_file = sc.textFile(tweets_path)
    network_file = sc.textFile(network_path)

    # get the uid->mid (where the mid is the_mid or the rt_mid)
    rdd_tweet_uid = tweets_file.flatMap(lambda line: split_uid_mid(line))

    # get the network relations
    rdd_network = network_file.flatMap(lambda line: split_users(line))


    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    #
    # get rt_uid -> rt_mid -> the_uid, 1
    rdd_retweets = tweets_file.flatMap(lambda line: split_retweets(line))\
            .flatMap(lambda words: split_dif(words, 1))
    rdd_counts = rdd_retweets.count()
    print "&&&&\n&&&&\n&&&&\n"
    print "now the real dif counts is %s" % (rdd_counts)
    if rdd_counts > 0 :
        # get rt_uid -> rt_mid -> the_uid, 0
        rdd_tweet_network = rdd_tweet_uid.leftOuterJoin(rdd_network)\
                .flatMap(lambda words: split_dif_0(words, 0))


        # combined the rdd_tweet_network and rdd_retweets together,
        # and the results in form of rt_uid -> rt_mid -> the_uid, 0/1
        rdd_result = rdd_tweet_network.leftOuterJoin(rdd_retweets)\
                .flatMap(lambda words: split_tweet_dif(words))

        rdd_check = rdd_result.flatMap(lambda line: split_check(line))

        rdd_counts = rdd_check.count()
        print "&&&&\n&&&&\n&&&&\n"
        tmp_str = "\nnow the real dif counts is %s" % (rdd_counts)
        print tmp_str
        log_write(tmp_str)


        stop_rdd = rdd_result.coalesce(1)
        stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
