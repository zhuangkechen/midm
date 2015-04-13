import sys

from pyspark import SparkContext, SparkConf

def resplit_libsvm(line) :
    #    0    1      2     3      4   5   6   7   8   9  10  11  12
    # "uid->mid->rtuid->time->hours->cT->cU->sI->sF->sP->sT->tP>dif"
    words = line.split("->")
    dif = words[12]
    sP = words[9]
    sT = words[10]
    sI = words[7]
    sF = words[8]
    cT = words[5]
    cU = words[6]
    tP = words[11]
    tD = words[4]
    ##
    the_time = words[3]
    month = the_time.split()[0].split("-")[1]
    days = the_time.split()[0].split("-")[2]
    if month == "05" and int(days) > 3 :
        tmp_str = "%s\t1:%s\t2:%s\t3:%s\t4:%s\t5:%s\t6:%s\t7:%s\t8:%s" % \
                (dif, sP, sT, sI, sF, cT, cU, tP, tD)
        yield (tmp_str)

def split_users(line):
    new_line = line.split("'")
    yield (new_line[1], 1)
    yield (new_line[3], 1)

def reduce_cunc(a, b):
    if a!=None and b!=None :
        return a+b

def log_write(counts):
    f = open("log.txt", 'a')
    f.write(counts)
    f.close()



if __name__ == "__main__":
    appName = "Kechen_divide_training"
    master = "spark://node06:7077"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)

    features_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/features_allin1"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/test_libsvm_tmp"

    #features_path= "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/features_sample"
    #features_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/features_allin1"
    #output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/libsvm_sample"
    #output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/libsvm_allin1"


    features_file = sc.textFile(features_path)

    rdd_dif_libsvm = features_file.flatMap(lambda line: resplit_libsvm(line))


    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"

    rdd_result =  rdd_dif_libsvm
    stop_rdd = rdd_result.coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
