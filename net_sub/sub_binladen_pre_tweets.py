import sys

from pyspark import SparkContext, SparkConf

def split_func(line):
    new_line = line.split("|#|")
    for words in new_line :
        if words.split(":")[0] == "time" :
            the_time = words.split(":")[1].split()[0]
            month = the_time.split("-")[1]
            day = the_time.split("-")[2]
            if month == "04" or int(day) <= 2 :
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
    tweets_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/binladen_tweets"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/binladen_tweets_pre"
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
