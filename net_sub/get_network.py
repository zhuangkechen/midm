import sys

from pyspark import SparkContext, SparkConf

def split_user(line):
    new_line = line.split("'")
    uid = new_line[1]
    counts = new_line[2].split()[1].split(")")[0]
    yield (uid, counts)

def split_link(line):
    new_line = line.split()
    yield (new_line[0], line)
    yield (new_line[1], line)

def split_count(words):
    uid = words[0]
    line = words[1][0]
    counts = words[1][1]
    if counts !=None :
        if int(counts) >=2 :
            yield (line, 1)

def split_start(word):
    if word[1] == 2 :
        yield (word[0])

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

    #raw_network_path = "hdfs://node06:9000/user/function/rawdata/finalsn"
    raw_network_path  = "hdfs://node06:9000/user/function/mb_analysis/the_network/network_2011_09_10"
    #raw_network_path= "hdfs://node06:9000/user/function/mb_analysis/the_network/retweet_network"
    tweet_user_path = "hdfs://node06:9000/user/function/mb_analysis/2011-09_to_10_retweet_users"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/network_tmp2"

    raw_network_file = sc.textFile(raw_network_path)
    retweet_user_file = sc.textFile(tweet_user_path)

    rdd_users = retweet_user_file.flatMap(lambda line: split_user(line))

    rdd_links = raw_network_file.flatMap(lambda line: split_link(line))

    rdd_result = rdd_links.leftOuterJoin(rdd_users)\
            .flatMap(lambda words: split_count(words))\
            .reduceByKey(lambda a,b: reduce_cunc(a,b))\
            .flatMap(lambda words: split_start(words))


    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    stop_rdd = rdd_result.coalesce(1)
    the_counts = stop_rdd.count()
    stop_rdd.saveAsTextFile(output_path)
    tmp_str = "\nthe links in retweet network  count is %d" % (the_counts)
    log_write(tmp_str)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
