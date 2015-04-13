import sys

from pyspark import SparkContext, SparkConf

def split_uid_mid(line):
    new_line = line.split("|#|")
    real_uid = ""
    for words in new_line :
        if words.split(":")[0] == "time" :
            the_time = words.split(":")[1].split("-")
            if the_time[0] != "2011" :
                break
            if int(the_time[1]) > 10 :
                break
        if words.split(":")[0] == "uid" :
            the_uid = words.split(":")[1].split("\t")
            real_uid = the_uid[0].split("$")[0]
        if words.split(":")[0] == "rtUid" :
            yield (real_uid, 1)
            the_uid = words.split(":")[1].split("\t")
            for uids in the_uid :
                uid = uids.split("$")
                yield (uid[0], 1)


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

    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/choosenTweetsGaddafi"
    output_path_1 = "hdfs://node06:9000/user/function/mb_analysis/2011-09_to_10_retweet_users"

    tweets_file = sc.textFile(tweets_path)

    rdd_result = tweets_file.flatMap(lambda line: split_uid_mid(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a,b))


    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    stop_rdd = rdd_result.coalesce(1)
    the_counts = stop_rdd.count()
    stop_rdd.saveAsTextFile(output_path_1)
    tmp_str = "\nall the users found in retweets in 2010-01 to 2010-10 count is %d" % (the_counts)
    log_write(tmp_str)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
