import sys

from pyspark import SparkContext, SparkConf

def split_tweets_feature(line) :
    new_line = line.split("|#|")
    cT = 0
    cU = 0
    mid = ""
    for words in new_line :
        if words.split(":")[0] == "eventList" :
            cT = 1
        if words.split(":")[0] == "isContainLink" :
            if words.split(":")[1] != "false" :
                cU = 1
        if words.split(":")[0] == "mid" :
            mid = words.split(":")[1]
    tmp_str = "%d->%d" % (cT, cU)
    yield (mid, tmp_str)
def resplit_libsvm(line) :
    #    0    1      2     3      4   5   6   7   8   9  10  11  12
    # "uid->mid->rtuid->time->hours->cT->cU->sI->sF->sP->sT->tP>dif"
    words = line.split("->")
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

    #user_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/user_follow_tweet_count"
    network_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/network_tmp2"
    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/tweets_tmp2"
    #
    retweet_count_mutual_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/user_retweet_count_with_mutual"
    #
    user_ft_period_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/user_follow_tweet_period"
    # here goes the output_path
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/features_allin1"
    libsvm_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/features_libsvm"
    # here is the dif total path
    dif_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/retweet_dif_with_time_hours"

    #user_file = sc.textFile(user_path)
    network_file = sc.textFile(network_path)
    #mutual_file = sc.textFile(mutual_path)
    tweets_file = sc.textFile(tweets_path)
    retweet_count_mutual_file = sc.textFile(retweet_count_mutual_path)
    user_ft_period_file = sc.textFile(user_ft_period_path)

    dif_file = sc.textFile(dif_path)

    # get the rdds
    rdd_dif_mid = dif_file.flatMap(lambda line: split_dif_mid(line))

    rdd_tweet_feature = tweets_file.flatMap(lambda line: split_tweets_feature(line))

    # add the cT and the cU into features. and return the link as the key.
    # "link\t" "uid->mid->rtuid->time->hours->cT->cU->dif"
    rdd_dif_link = rdd_dif_mid.leftOuterJoin(rdd_tweet_feature)\
            .flatMap(lambda line: resplit_tweets_feature(line))

    rdd_link_feature = retweet_count_mutual_file.flatMap(lambda line: split_count_mutual(line))
    # add the sI and the sF into features. and return the userid as the key.
    # "uid\t" "uid->mid->rtuid->time->hours->cT->cU->sI->sF->dif"
    rdd_dif_uid = rdd_dif_link.leftOuterJoin(rdd_link_feature)\
            .flatMap(lambda line: resplit_link_feature(line))

    rdd_uid_feature = user_ft_period_file.flatMap(lambda line: split_users(line))
    # add the sP, sT and tP into features. and return the total.
    # "uid->mid->rtuid->time->hours->cT->cU->sI->sF-sP->sT->tP>dif"
    rdd_dif= rdd_dif_uid.leftOuterJoin(rdd_uid_feature)\
            .flatMap(lambda line: resplit_final(line))

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


    rdd_result = rdd_dif
    #stop_rdd = rdd_result.coalesce(1)
    stop_rdd = rdd_result
    stop_rdd.saveAsTextFile(output_path)
    #stop_rdd = rdd_dif_libsvm.coalesce(1)
    #stop_rdd.saveAsTextFile(libsvm_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
