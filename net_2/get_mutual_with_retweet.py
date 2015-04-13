import sys

from pyspark import SparkContext, SparkConf
def split_mutual(line):
    new_line = line.split()
    u1 = new_line[0]
    u2 = new_line[1]
    tmp_str = "%s->%s" % (u1, u2)
    yield (tmp_str, 1)
    tmp_str = "%s->%s" % (u2, u1)
    yield (tmp_str, 1)

def split_users(line):
    new_line = line.split("'")
    link = new_line[1]
    counts = new_line[2].split()[1].split(")")[0]
    yield (link, counts)

def reduce_cunc(a, b):
    if a!=None and b!=None :
        return a+b

def resplit_users(words):
    link = words[0]
    counts = words[1][0]
    mutual = words[1][1]
    if mutual == None :
        mutual = 0
    else :
        mutual = 1
    tmp_str = "%s->%s->%d" % (link, counts, mutual)
    yield (tmp_str)

def log_write(counts):
    f = open("log.txt", 'a')
    f.write(counts)
    f.close()



if __name__ == "__main__":
    appName = "Kechen_get_gaddafi_users"
    master = "spark://node06:7077"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)


    network_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/network_tmp2"
    mutual_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/network_mutual"
    retweets_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/user_retweet_2011_from"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/user_retweet_count_with_mutual"

    mutual_file = sc.textFile(mutual_path)
    retweets_file = sc.textFile(retweets_path)

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
    rdd_mutual = mutual_file.flatMap(lambda line: split_mutual(line))
    rdd_tweet_counts = retweets_file.flatMap(lambda line: split_users(line))
    rdd_result = rdd_tweet_counts.leftOuterJoin(rdd_mutual, 1)\
            .flatMap(lambda words: resplit_users(words))
    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
