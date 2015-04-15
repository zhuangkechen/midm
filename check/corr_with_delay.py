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
        tmp_str = "%s->%s->%s" % (rt_uid, rt_mid, the_uid[0])
        yield (tmp_str, hours)

    if rt_uid != None and rt_time !=None and len(the_uid) > 1 :
        tmp_hour = hours/len(the_uid)
        for i in range(0, len(the_uid)-2) :
            tmp_str = "%s->%s->%s" % (the_uid[i+1], rt_mid, the_uid[i])
            yield (tmp_str, hours)
        tmp_str = "%s->%s->%s" % (rt_uid, rt_mid, the_uid[-1])
        yield (tmp_str, hours)


def resplit_libsvm(line) :
    #    0    1      2     3      4   5   6   7   8   9  10  11  12
    # "rtuid->mid->uid->time->hours->cT->cU->sI->sF->sP->sT->tP>dif"
    words = line.split("->")
    rt_uid = words[0]
    mid = words[1]
    uid = words[2]
    #
    dif = words[12]
    sP = words[9]
    sT = words[10]
    sI = words[7]
    sF = words[8]
    cT = words[5]
    cU = words[6]
    tP = words[11]
    tD = words[4]
    ##
    tmp_str = "%s\t1:%s\t2:%s\t3:%s\t4:%s\t5:%s\t6:%s\t7:%s\t8:%s" % \
            (dif, sP, sT, sI, sF, cT, cU, tP, tD)
    tmp_str_1 = "%s->%s->%s" % (rt_uid, mid, uid)
    yield (tmp_str_1, tmp_str)

def split_users(line):
    new_line = line.split("'")
    yield (new_line[1], new_line[3])

def resplit_tweets_feature(words):
    the_ids = words[0]
    features = words[1][0]
    delays = words[1][1]
    if features == None or delays == None :
        return
    tmp_str = "%s\t%s" % (features, delays)
    yield (tmp_str)

def split_start(word):
    if word[1][1] != None :
        yield (word[1][1])

def reduce_cunc(a, b):
    if a!=None and b!=None :
        return a+b

def reduce_small(a, b):
    if a!=None and b!=None :
        if a > b:
            return b
        else :
            return a

def reduce_svm(a, b):
    if a!=None and b!=None :
        tD_a = float(a.split()[8].split(":")[1])
        tD_b = float(b.split()[8].split(":")[1])
        if tD_a > tD_b:
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

    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/tweets_tmp2"
    #
    # here goes the output_path
    features_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/features_allin1"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/delay_libsvm"

    tweets_file = sc.textFile(tweets_path)
    features_file = sc.textFile(features_path)

    # get the rdds
    rdd_tweets = tweets_file.flatMap(lambda line: split_retweets(line))\
                 .reduceByKey(lambda a,b: reduce_small(a,b))
    rdd_features = features_file.flatMap(lambda line: resplit_libsvm(line))\
                 .reduceByKey(lambda a,b: reduce_svm(a,b))

    rdd_result = rdd_features.leftOuterJoin(rdd_tweets)\
                 .flatMap(lambda words: resplit_tweets_feature(words))

    #rdd_dif_libsvm = rdd_dif.flatMap(lambda line: resplit_libsvm(line))



    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    #print "&&&&\n&&&&\n&&&&\n"
    #tmp_str = "\nnow the real dif counts is %s" % (rdd_counts)
    #print tmp_str
    #log_write(tmp_str)


    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    #stop_rdd = rdd_dif_libsvm.coalesce(1)
    #stop_rdd.saveAsTextFile(libsvm_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
