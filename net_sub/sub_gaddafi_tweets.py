import sys

from pyspark import SparkContext, SparkConf

def split_func(line):
    new_line = line.split("|#|")
    for words in new_line :
        if words.split(":")[0] == "eventList" :
            the_event = words.split(":")[1].split("$")
            for event in the_event :
                if event == "death of Muammar Gaddafi" :
                    yield (line)
                    break
        if words.split(":")[0] == "rtEventList" :
            the_event = words.split(":")[1].split("$")
            for event in the_event :
                if event == "death of Muammar Gaddafi" :
                    yield (line)
                    break

def split_users(line):
    new_line = line.split("'")
    yield (new_line[1], 1)
    yield (new_line[3], 1)


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
    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/0910_analysis/tweets_tmp2"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/0910_analysis/gaddafi_tweets"
    tweets_file = sc.textFile(tweets_path)
    rdd_tweets = tweets_file.flatMap(lambda line: split_func(line))

    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    stop_rdd = rdd_tweets.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
