import sys

from pyspark import SparkContext, SparkConf
def split_start(line):
    words = line.split("->")
    if words[-1] == "1" :
        yield(line)

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

    check_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/retweet_dif_with_time_hours"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/retweet_dif_sample"

    check_file = sc.textFile(check_path)


    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    rdd_result = check_file.flatMap(lambda line: split_start(line))


    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
