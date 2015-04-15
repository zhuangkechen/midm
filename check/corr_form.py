import sys

from pyspark import SparkContext, SparkConf

def resplit_libsvm(line) :
    #    0    1      2     3      4   5   6   7   8   9  10  11  12
    # "uid->mid->rtuid->time->hours->cT->cU->sI->sF->sP->sT->tP>dif"
    words = line.split()
    dif = int(words[0])
    sP = float(words[1].split(":")[1])
    sT = float(words[2].split(":")[1])
    sI = float(words[3].split(":")[1])
    sF = float(words[4].split(":")[1])
    cT = float(words[5].split(":")[1])
    cU = float(words[6].split(":")[1])
    tP = float(words[7].split(":")[1])
    tD = float(words[8].split(":")[1])

    real_delay = float(words[8])
    ##
    if dif == 1:
        yield ("sP", real_delay*sP )
        yield ("sT", real_delay*sT )
        yield ("sI", real_delay*sI )
        yield ("sF", real_delay*sF )
        yield ("cT", real_delay*cT )
        yield ("cU", real_delay*cU )
        yield ("tP", real_delay*tP )
        yield ("tD", real_delay*tD )

def split_counts(words, total_count):
    feature_name = words[0]
    counts = float(words[1]) / total_count
    yield (feature_name, counts)

def reduce_cunc(a, b):
    if a!=None and b!=None :
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

    features_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/delay_libsvm"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/delay_corr"

    #features_path= "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/features_sample"
    #features_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/features_allin1"
    #output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/libsvm_sample"
    #output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/libsvm_allin1"


    features_file = sc.textFile(features_path)
    total_count = features_file.count()

    tmp_str = "total_count of features all is %d" % (total_count)
    log_write(tmp_str)
    print(tmp_str)

    rdd_dif_libsvm = features_file.flatMap(lambda line: resplit_libsvm(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a,b))\
            .flatMap(lambda words: split_counts(words, total_count))\
            .coalesce(1)\
            .reduceByKey(lambda a,b: reduce_cunc(a,b))


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
