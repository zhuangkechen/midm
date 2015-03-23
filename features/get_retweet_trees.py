import sys

from pyspark import SparkContext, SparkConf

def split_uid_mid(line):
    new_line = line.split("|#|")
    the_uid = ""
    rt_uid = None
    rt_mid = ""
    for words in new_line :
        if words.split(":")[0] == "mid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "rtMid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "uid" :
            the_uid = words.split(":")[1].split("\t")[0].split("$")[0]

        if words.split(":")[0] == "rtUid" :
            rt_uid = words.split(":")[1].split("\t")[0].split("$")[0]
    yield (the_uid, rt_mid)

def split_retweets(line):
    new_line = line.split("|#|")
    the_uid = ""
    rt_uid = None
    rt_mid = ""
    for words in new_line :
        if words.split(":")[0] == "mid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "rtMid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "uid" :
            the_uid = words.split(":")[1].split("\t")[0].split("$")[0]

        if words.split(":")[0] == "rtUid" :
            rt_uid = words.split(":")[1].split("\t")[0].split("$")[0]
    if rt_uid != None :
        yield (rt_uid, (rt_mid, the_uid))

def split_dif(words, dif) :
    the_uid = words[0]
    rt_mid = words[1][0]
    rt_uid = words[1][1]
    if rt_mid != None and rt_uid != None :
        tmp_str = "%s->%s->%s" % (the_uid, rt_mid, rt_uid)
        yield (tmp_str, dif)

def split_result(words) :
    msg = words[0].split("->")
    the_uid = msg[0]
    rt_mid = msg[1]
    rt_uid = msg[2]
    def_0 = words[1][0]
    def_1 = words[1][1]
    if def_1 == None :
        dif = 0
    else :
        dif = 1
    tmp_str = "%s->%s->%s->%d" % (the_uid, rt_mid, rt_uid, dif)
    yield (tmp_str)

def split_users(line):
    new_line = line.split("'")
    yield (new_line[1], new_line[3])


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
    output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/retweet_dif"

    tweets_file = sc.textFile(tweets_path)
    network_file = sc.textFile(network_path)

    rdd_tweet_uid = tweets_file.flatMap(lambda line: split_uid_mid(line))
    rdd_network = network_file.flatMap(lambda line: split_users(line))


    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    rdd_retweets = tweets_file.flatMap(lambda line: split_retweets(line))\
            .flatMap(lambda words: split_dif(words, 1))

    rdd_tweet_network = rdd_tweet_uid.leftOuterJoin(rdd_network)\
            .flatMap(lambda words: split_dif(words, 0))

    rdd_result = rdd_tweet_network.leftOuterJoin(rdd_retweets)\
            .flatMap(lambda words: split_result(words))
    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
