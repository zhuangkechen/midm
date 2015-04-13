import sys

from pyspark import SparkContext, SparkConf


def split_link(line):
    new_line = line.split()
    yield (new_line[0], new_line[1])

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

    network_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/network_tmp1"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/network_tmp2"

    network_file = sc.textFile(network_path)


    rdd_links = network_file.flatMap(lambda line: split_link(line))

    rdd_result = rdd_links

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
    tmp_str = "\nthe links in network  count is %d" % (the_counts)
    log_write(tmp_str)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
