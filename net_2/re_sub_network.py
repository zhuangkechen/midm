import sys

from pyspark import SparkContext, SparkConf
def split_start(line):
    yield(line, 1)

def reduce_cunc(a, b):
    if a!=0 and b!=0 :
        return a+b

def resplit(words):
    yield(words[0])

def log_write(counts):
    f = open("log.txt", 'a')
    f.write(counts)
    f.close()



if __name__ == "__main__":
    appName = "Kechen_get_gaddafi_users"
    master = "spark://node06:7077"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)


    network_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/net_sub/network_tmp2"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/network_tmp1"
    check_file = sc.textFile(network_path)


    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    rdd_check = check_file.flatMap(lambda line: split_start(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a,b), 1)\
            .flatMap(lambda words: resplit(words))
    stop_rdd = rdd_check.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
