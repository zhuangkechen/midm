import sys

from pyspark import SparkContext, SparkConf

def split_periods(line):
    new_line = line.split("|#|")
    the_uid = None
    the_time = 0
    for words in new_line :
        if words.split(":")[0] == "uid" :
            the_uid = words.split(":")[1].split("\t")[0].split("$")[0]
        if words.split(":")[0] == "time" :
            the_time = words.split(":")[1].split()[1]
            the_time = int(the_time) % 6
    if the_uid != None :
        tmp_str = "%s->%d" % (the_uid, the_time)
        yield (tmp_str, 1)

def resplit_periods(words):
    if words[1] != None :
        the_uid = words[0].split("->")[0]
        the_time= int(words[0].split("->")[1])
        t_count = int(words[1])
        tmp_str = "%d->%d" % (the_time, t_count)
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
            tmp_list = i.split("->")
            t_time = int(tmp_list[0])
            t_count = int(tmp_list[1])
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
    #tweets_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/tweets_tmp2"
    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/choosen2011Gaddafi"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/retweet_periods_2011"
    #output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/retweet_periods"

    tweets_file = sc.textFile(tweets_path)
    network_file = sc.textFile(network_path)


    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"

    rdd_result = tweets_file.flatMap(lambda line: split_periods(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a, b))\
            .flatMap(lambda words: resplit_periods(words))\
            .groupByKey().flatMap(lambda words: resplit_result(words))

    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
