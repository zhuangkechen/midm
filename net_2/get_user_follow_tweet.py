import sys

from pyspark import SparkContext, SparkConf
def split_users(line):
    new_line = line.split("'")
    u1 = new_line[1]
    u2 = new_line[3]
    yield (u1, 1)
    yield (u2, 1)

def split_tweets_users(line):
    new_line = line.split("|#|")
    for words in new_line :
        if words.split(":")[0] == "uid" :
            uid = words.split(":")[1].split()[0].split("$")[0]
            yield (uid, 1)

def split_period(line):
    new_line = line.split("'")
    uid = new_line[1]
    i = new_line[3].split("->")
    the_period = "%s||%s||%s||%s||%s||%s" % (i[0], i[1], i[2], i[3], i[4], i[5])
    yield (uid, the_period)

def split_one(words):
    yield (1, words[1])

def reduce_cunc(a, b):
    if a!=None and b!=None :
        return a+b

def reduce_max(a, b):
    if a!=None and b!=None :
        if a > b :
            return a
        else :
            return b

def resplit_users(words, follow_max):
    uid = words[0]
    follows = words[1][0]
    tweets = words[1][1]
    hours = (30+31) * 24
    if follows == None :
        follows = 0
    else :
        follows = int(follows)/(follow_max*1.0)
        if follows > 1 :
            follows = 1.0
    if tweets == None :
        tweets = 0
    else :
        tweets = int(tweets)/(hours * 1.0)
        if tweets > 1 :
            tweets = 1.0
    tmp_str = "%f->%f" % (round(follows, 5), round(tweets, 5))
    yield (uid, tmp_str)

def resplit_all(words):
    uid = words[0]
    ft = words[1][0]
    period = words[1][1]
    if period == None :
        period = "None"
    tmp_str = "%s->%s" % (ft, period)
    yield (uid, tmp_str)

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
    network_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/network_tmp2"
    period_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/retweet_periods_2011"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/user_follow_tweet_period"

    network_file = sc.textFile(network_path)
    tweets_file = sc.textFile(tweets_path)
    period_file = sc.textFile(period_path)

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
    rdd_follow = network_file.flatMap(lambda line: split_users(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a,b), 1)
    rdd_f_max = rdd_follow.flatMap(lambda line: split_one(line))\
            .reduceByKey(lambda a,b: reduce_max(a,b), 1)
    rdd_tweets = tweets_file.flatMap(lambda line: split_tweets_users(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a,b), 1)
    rdd_period = period_file.flatMap(lambda line: split_period(line))

    follow_max = int(rdd_f_max.collectAsMap()[1])
    rdd_result = rdd_follow.leftOuterJoin(rdd_tweets, 1)\
            .flatMap(lambda words: resplit_users(words, follow_max))\
            .leftOuterJoin(rdd_period, 1)\
            .flatMap(lambda words: resplit_all(words))
    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
