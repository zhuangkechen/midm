import sys

from pyspark import SparkContext, SparkConf

def split_tweets(line):
    new_line = line.split("|#|")
    the_uid = None
    the_period = 0
    for words in new_line :
        word = words.split(":")
        if word[0] == "time" :
            the_date = word[1].split("-")
            if int(the_date[0]) > 2011 :
                break
            else :
                the_period = int(word[1].split()[1])
                the_period = the_period % 6
        if word[0] == "uid" :
            the_uid = word[1].split("\t")[0].split("$")[0]
            yield (the_uid, the_period)
            break

def split_periods(words):
    if words[1][1]!= None :
        the_uid = words[0]
        the_time = int(words[1][1])
        tmp_str = "%s->%d" % (the_uid, the_time)
        yield (tmp_str, 1)

def resplit_periods(words):
    if words[1] != None :
        the_uid = words[0].split("->")[0]
        t_period = words[0].split("->")[1]
        t_count = int(words[1])
        tmp_str = "%s->%d" % (t_period, t_count)
        yield(the_uid, tmp_str)

def resplit_result(words):
    if words[1] != None :
        the_uid = words[0]
        t_period = [0,0,0,0,0,0]
        t_total = 0
        the_list = words[1]
        #if not isinstance(words[1],list) :
        #    the_list = []
        #    the_list.append(words[1])
        for i in the_list :
            tmp_str = i.split("->")
            t_time = int(tmp_str[0])
            t_count = int(tmp_str[1])
            t_period[t_time] = t_count
            t_total = t_total + t_count
        tmp_str = ""
        for i in range(6):
            t_count = t_period[i]/(t_total * 1.0)
            if i == 0 :
                tmp_str = "%f" % (round(t_count, 5))
            else :
                the_tmp = "->%f" % (round(t_count, 5))
                tmp_str = tmp_str + the_tmp
        yield(the_uid, tmp_str)

def split_users(line):
    new_line = line.split("'")
    yield (new_line[1], 1)
    yield (new_line[3], 1)

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

    raw_path = "hdfs://node06:9000/user/function/rawdata/microblogs/sortedmicroblogs.txt"
    network_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_tmp2"
    #tweets_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/tweets_tmp2"
    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/choosen2011Gaddafi"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/retweet_periods_raw"


    tweets_file = sc.textFile(raw_path)
    network_file = sc.textFile(network_path)


    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    rdd_users= network_file.flatMap(lambda line: split_users(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a, b))

    rdd_tweets = tweets_file.flatMap(lambda line: split_tweets(line))

    rdd_result = rdd_users.leftOuterJoin(rdd_tweets)\
            .flatMap(lambda words: split_periods(words))\
            .reduceByKey(lambda a,b: reduce_cunc(a, b))\
            .flatMap(lambda words: resplit_periods(words))\
            .groupByKey().flatMap(lambda words: resplit_result(words))

    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
