import sys

from pyspark import SparkContext, SparkConf
def split_start(line):
    yield(line, 1)

def resplit(words):
    if words[1] >= 2 :
        yield(words[0], 2)

def resplit_1(words):
    if words[1] == 1 :
        yield(words[0], 1)
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

    check_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/network_tmp2"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/check_out"

    check_file = sc.textFile(check_path)


    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    rdd_check = check_file.flatMap(lambda line: split_start(line))\
            .reduceByKey(lambda a,b:reduce_cunc(a,b), 1)
    check_2 = rdd_check.flatMap(lambda words: resplit(words))
    #check_1 = rdd_check.flatMap(lambda words: resplit_1(words))

    #counts = check_1.count()
    #tmp_str = "\nthe check unsame count is :%d" % (counts)
    #log_write(tmp_str)
    #print tmp_str

    counts = check_2.count()
    tmp_str = "\nthe check same count is :%d" % (counts)
    log_write(tmp_str)
    print tmp_str
    #stop_rdd = rdd_check.coalesce(1)
    #stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
