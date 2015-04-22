import sys
from datetime import *

from pyspark import SparkContext, SparkConf
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

    added_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/features_allin2"
    origin_path = "hdfs://node06:9000/user/function/mb_analysis/0910_analysis/features_allin1"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/features_allin1"

    added_file = sc.textFile(added_path)
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
    # get rt_uid -> rt_mid -> the_uid, 0
    rdd_merged = origin_file.union(added_file)\
            .flatMap(lambda words: resplit_union(words))\
            .reduceByKey(lambda a,b: reduce_dif(a,b))\
            .flatMap(lambda words: resplit_result(words))


    stop_rdd = rdd_merged
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
