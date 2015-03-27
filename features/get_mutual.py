import sys

from pyspark import SparkContext, SparkConf
def split_users(line):
    new_line = line.split("'")
    u1 = new_line[1]
    u2 = new_line[3]
    tmp_str = ""
    if new_line[0][0]<new_line[1][0] :
        tmp_str = "%s->%s" % (u1, u2)
    else :
        tmp_str = "%s->%s" % (u2, u1)
    yield (tmp_str, 1)

def reduce_cunc(a, b):
    if a!=0 and b!=0 :
        return a+b

def resplit_users(words):
    if int(words[1]) == 2 :
        u1 = words[0].split("->")[0]
        u2 = words[0].split("->")[1]
        tmp_str = "%s\t%s" % (u1, u2)
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


    network_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_tmp2"
    mutual_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_mutual"

    network_file = sc.textFile(network_path)

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
    rdd_result = network_file.flatMap(lambda line: split_users(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a,b))\
            .flatMap(lambda words: resplit_users(words))
    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(mutual_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
