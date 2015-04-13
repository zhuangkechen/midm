import sys

from pyspark import SparkContext, SparkConf

def split_users(line):
    new_line = line.split("'")
    yield (new_line[1], 1)
    yield (new_line[3], 1)

def split_start(word):
    if word[1] >= 1 :
        u1 = word[0].split()[0]
        u2 = word[0].split()[1]
        yield (u1, u2)

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

    network_path1 = "hdfs://node06:9000/user/function/mb_analysis/0910_analysis/network_tmp2"
    output_path1 = "hdfs://node06:9000/user/function/mb_analysis/0910_analysis/network_user_count"
    network_path2 = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/network_tmp2"
    output_path2 = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/network_user_count"
    #output_path = "hdfs://node06:9000/user/function/mb_analysis/2011_01_10_retweet_links"

    raw_network_file1 = sc.textFile(network_path1)
    raw_network_file2 = sc.textFile(network_path2)

    rdd_links = raw_network_file1.flatMap(lambda line: split_users(line))
    rdd_result = rdd_links.reduceByKey(lambda a,b: reduce_cunc(a,b))
    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path1)


    rdd_links = raw_network_file2.flatMap(lambda line: split_users(line))
    rdd_result = rdd_links.reduceByKey(lambda a,b: reduce_cunc(a,b))
    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path2)
    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
