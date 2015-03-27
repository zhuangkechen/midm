import sys

from pyspark import SparkContext, SparkConf

def split_retweet_from(line):
    new_line = line.split("|#|")
    the_uid = None
    for words in new_line :
        if words.split(":")[0] == "uid" :
            the_uid = words.split(":")[1].split("\t")[0].split("$")[0]
    if the_uid != None :
        yield (the_uid, 1)

def split_retweet_to(line):
    new_line = line.split("|#|")
    rt_uid = None
    for words in new_line :
        if words.split(":")[0] == "rtUid" :
            rt_uid = words.split(":")[1].split("\t")[0].split("$")[0]
    if rt_uid != None :
        yield (rt_uid, 1)

def split_retweet_full(line):
    new_line = line.split("|#|")
    rt_uid = None
    the_uid = None
    for words in new_line :
        if words.split(":")[0] == "uid" :
            the_uid = words.split(":")[1].split("\t")[0].split("$")[0]
        if words.split(":")[0] == "rtUid" :
            rt_uid = words.split(":")[1].split("\t")[0].split("$")[0]
    if rt_uid != None :
        tmp_str = "%s->%s" % (rt_uid, the_uid) #changed 2015-03-25
        yield (tmp_str, 1)

def split_users(line):
    new_line = line.split("'")
    yield (new_line[1], 1)
    yield (new_line[3], 1)

def split_relations(line):
    new_line = line.split("'")
    tmp_str = "%s->%s" % (new_line[1], new_line[3])
    yield (tmp_str, 1)

def resplit_users(words):
    the_uid = words[0]
    degree = words[1][0]
    the_retweets = words[1][0]
    if the_retweets == None :
        the_retweets = 0
    yield (the_uid, the_retweets)

def resplit_relations(words):
    the_uid = words[0]
    degree = words[1][0]
    the_retweets = words[1][0]
    if the_retweets == None :
        the_retweets = 0
    from_uid = the_uid.split("->")[0]
    to_uid = the_uid.split("->")[1]
    yield (from_uid, (to_uid, the_retweets))

def resplit_result(words):
    from_uid = words[0]
    to_uid = words[1][0][0]
    s_count = words[1][0][1]
    t_count = words[1][1]
    the_retweets = 0
    if t_count == None or s_count == None :
        the_retweets = 0
    elif t_count == 0 or s_count == 0 :
        the_retweets = 0
    else :
        the_retweets = s_count / (t_count * 1.0)
        the_retweets = round(the_retweets, 5)
    tmp_str = "%s->%s" % (from_uid, to_uid)
    yield (tmp_str, the_retweets)

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
    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/choosen2011Gaddafi"
    retweets_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/user_retweet_2011_from"

    tweets_file = sc.textFile(tweets_path)
    network_file = sc.textFile(network_path)

    rdd_network = network_file.flatMap(lambda line: split_relations(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a, b))
    rdd_users= network_file.flatMap(lambda line: split_users(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a, b))

    rdd_retweet_from = tweets_file.flatMap(lambda line: split_retweet_from(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a, b))
    rdd_retweet_full = tweets_file.flatMap(lambda line: split_retweet_full(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a, b))

    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    print "****************************************************\n"
    print "Here to get the User From Retweet counts!\n"
    print "****************************************************\n"
    print "\n\n"
    rdd_from = rdd_users.leftOuterJoin(rdd_retweet_from)\
            .flatMap(lambda words: resplit_users(words))
    print "****************************************************\n"
    print "Here to get the User Full Retweet counts!\n"
    print "****************************************************\n"
    print "\n\n"
    rdd_full = rdd_network.leftOuterJoin(rdd_retweet_full)\
            .flatMap(lambda words: resplit_relations(words))
    print "****************************************************\n"
    print "Here to combine the  Retweet counts!\n"
    print "****************************************************\n"
    print "\n\n"
    rdd_result = rdd_full.leftOuterJoin(rdd_from)\
            .flatMap(lambda words: resplit_result(words))
    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(retweets_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
