import sys

from pyspark import SparkContext, SparkConf

def split_func(line):
    new_line = line.split("|#|")
    for words in new_line :
        if words.split(":")[0] == "time" :
            the_time = words.split(":")[1]
            year = the_time.split("-")[0]
            month = the_time.split("-")[1]
            if year != "2011" :
                break
            if month != "04" and month != "05" :
                break
        if words.split(":")[0] == "uid" :
            the_uid = words.split(":")[1].split("\t")[0].split("$")[0]
            yield (the_uid, line)
            break

def split_users(line):
    new_line = line.split("'")
    yield (new_line[1], 1)
    yield (new_line[3], 1)


def split_start(word):
    if word[1][1] != None :
        yield (word[1][1])

def reduce_cunc(a, b):
    if a!=None and b!=None :
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
    tweets_path = "hdfs://node06:9000/user/function/rawdata/microblogs/sortedmicroblogs.txt"
    #tweets_path = "hdfs://node06:9000/user/function/mb_analysis/choosenTweetsGaddafi"
    network_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/network_tmp2"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/tweets_tmp2"
    tweets_file = sc.textFile(tweets_path)
    network_file = sc.textFile(network_path)
    rdd_tweets = tweets_file.flatMap(lambda line: split_func(line))
    rdd_network = network_file.flatMap(lambda line: split_users(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a,b), 1)
    rdd_result = rdd_network.leftOuterJoin(rdd_tweets)\
            .flatMap(lambda line: split_start(line))

    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
