import sys

from pyspark import SparkContext, SparkConf

def split_dif_mid(line) :
    mid = line.split("->")[1]
    yield (mid, line)

def split_tweets_feature(line) :
    new_line = line.split("|#|")
    cT = 0
    cU = 0
    mid = ""
    for words in new_line :
        if words.split(":")[0] == "eventList" :
            cT = 1
        if words.split(":")[0] == "isContainLink" :
            if words.split(":")[1] != "false"
                cU = 1
        if words.split(":")[0] == "mid" :
            mid = words.split(":")[1]
    tmp_str = "%d->%d" % (cT, cU)
    yield (mid, tmp_str)

def resplit_tweets_feature(words) :
    mid = words[0]
    dif_feature = words[1][0]
    tweet_feature = words[1][1]
    if tweet_feature == None :
        tweet_feature = "0->0"
    dif = dif_feature.split("->")[-1]
    tmp_str_1 = "%d->%d" % (cT, cU)
    tmp_str = dif_feature[:(len(dif_feature)-1)]
    tmp_str = tmp_str + tweet_feature + "->" + dif
    # get the link
    u1 = dif_feature.split("->")[0]
    u2 = dif_feature.split("->")[3]
    tmp_link = "%s->%s" % (u1, u2)
    yield (tmp_link, tmp_str)

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

def split_tweet_dif(words) :
    msg = words[0].split("->")
    the_uid = msg[0]
    rt_mid = msg[1]
    rt_uid = msg[2]
    the_time = words[1][0]
    def_1 = words[1][1]
    dif = 0
    if def_1 == None :
        dif = 0
    else :
        dif = 1
    tmp_str  = "%s->%s->%s->%s->%d" % (the_uid, rt_mid, rt_uid, the_time, dif)
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

    user_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/user_follow_tweet_count"
    network_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_tmp2"
    mutual_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_mutual"
    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/tweets_tmp2"
    retweet_count_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/user_retweet_2011_from"
    retweet_period_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/retweet_periods_2011"
    # here goes the output_path
    output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/features_allin1"
    # here is the dif total path
    dif_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/retweet_dif_with_time_hours"

    user_file = sc.textFile(user_path)
    network_file = sc.textFile(network_path)
    mutual_file = sc.textFile(mutual_path)
    tweets_file = sc.textFile(tweets_path)
    retweet_count_file = sc.textFile(retweet_count_path)
    retweet_period_file= sc.textFile(retweet_period_path)

    dif_file = sc.textFile(dif_path)

    # get the rdds
    rdd_dif_mid = dif_file.flatMap(lambda line: split_dif_mid(line))

    rdd_tweet_feature = tweets_file.flatMap(lambda line: split_tweets_feature(line))
    
    # add the cT and the cU into features. and return the link as the key.
    # "link\t" "uid->mid->rtuid->time->hours->cT->cU->dif" 
    rdd_dif_link = rdd_dif_mid.leftOuterJoin(rdd_tweet_feature)\
            .flatMap(lambda line: resplit_tweets_feature(line))

    # add the cT and the cU into features. and return the link as the key.
    # "link\t" "uid->mid->rtuid->time->hours->cT->cU->dif" 
    rdd_dif_link = rdd_dif_mid.leftOuterJoin(rdd_tweet_feature)\
            .flatMap(lambda line: resplit_tweets_feature(line))







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
    tmp_str = "\nnow the real dif counts is %s" % (rdd_counts)
    print tmp_str
    log_write(tmp_str)


    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
