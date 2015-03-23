import sys

from pyspark import SparkContext, SparkConf


# the split functions
#
def split_tweets(line):
    new_line = line.split("|#|")
    for words in new_line :
        if words.split(":")[0] == "uid" :
            the_uid = words.split(":")[1].split("\t")[0].split("$")[0]
            yield (the_uid, line)
            break

def split_user_from_network(line, choice) :
    words = line.split("'")
    if choice = 1 :
        yield (words[1], words[3])
    else :
        yield (words[1], 1)
        yield (words[3], 1)

def split_count(word):
    yield (word[0], 1)

###########################################

# the feature functions
#

# get the sP feature
def fea_sP(word, C_theta) :
    user_id = word[0]
    the_sP = int(word[1])/(C_theta*1.0)
    if the_sP >=1 :
        the_sP = 1.0
    yield (user_id, the_sP)


# get the sT feature
def fea_sT(word, C_sigma) :
    user_id = word[0]
    tweets_count = word[1][1]
    if tweets_count == None :
        the_sT = 0
    else :
        the_sT = int(word[1][1])/(C_sigma*1.0)
    if the_sT >=1 :
        the_sT = 1.0
    yield (user_id, the_sT)

def map_func(word):
    if word[1][1] == 1 :
        yield (word[0], word[1][0])

def map_count(word):
    yield (word[0], 1)
    yield (word[1], 1)

def reduce_cunc(a, b):
    if a!=0 and b!=0 :
        return a+b

def log_write(counts):
    f = open("log.txt", 'a')
    f.write(counts)
    f.close()



if __name__ == "__main__":
    appName = "Kechen_gaddafi_tweets_features_tmp1"
    master = "spark://node06:7077"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    network_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_tmp2"
    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/tweets_tmp2"

    tweets_file = sc.textFile(tweets_path)
    network_file = sc.textFile(network_path)

    rdd_users = network_file.flatMap(lambda line: split_user_from_network(line, 2))\
            .reduceByKey(lambda a,b: reduce_cunc(a,b))

    rdd_network = network_file.flatMap(lambda line: split_user_from_network(line, 1))

    rdd_tweets = tweets_file.flatMap(lambda line: split_tweets(line))
    rdd_tweets_count = rdd_tweets.flatMap(lambda word: split_count(word))\
            .reduceByKey(lambda a,b: reduce_cunc(a,b))

    # sP: get the counts sP = |F(u)|/theta
    C_theta = 1000
    rdd_sP = rdd_users.flatMap(lambda word: fea_sP(word, C_theta))

    # sT: get the tweet frequency sT = |M(u)|/sigma
    C_sigma = (30+31)*24
    rdd_sT = rdd_users.leftOuterJoin(rdd_tweets_count)\
            .flatMap(lambda word: fea_sT(word, C_sigma))

    # sI: get the retweet Interaction sI = |rM(u,v)|/|rM(u)|
    C_sigma = (30+31)*24

    rdd_result = rdd_network.leftOuterJoin(rdd_tweets)\
            .flatMap(lambda line: split_start(line))

    # Here start the loop
    print "######################################################\n"
    print "######################################################\n"
    print "#########               Start!!!               #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"

    for i in range(0, 20):
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
        print "This is the %d-th loop, let's wait..................\n" % (i+1)
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
        print "\n\n"
        loop = loop.groupByKey().flatMap(lambda word: re_split(word))
        stop_rdd = loop.flatMap(lambda word: map_func(word))\
                .coalesce(1)
        count_rdd = stop_rdd.flatMap(lambda word: map_count(word))\
                .reduceByKey(lambda a, b: reduce_cunc(a,b))\
                .coalesce(1)
        the_count = count_rdd.count()
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
        tmp_s =  "This is the %d-th loop, and the network now have %d users!!!!!!!!!!!!!!!!!!\n" % (i+1, the_count)
        print tmp_s
        log_write(tmp_s)
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
        print "\n\n"

    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"

    stop_rdd = loop.flatMap(lambda word: map_func(word)).coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
