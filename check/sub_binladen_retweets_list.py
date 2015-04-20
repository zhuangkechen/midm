import sys
from datetime import *

from pyspark import SparkContext, SparkConf

from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils

def split_retweets(line):
    new_line = line.split("|#|")
    the_uid = []
    rt_uid = None
    rt_mid = ""
    the_time = ""
    rt_time = None
    for words in new_line :
        if words.split(":")[0] == "time" :
            the_time = words[5 :]

        if words.split(":")[0] == "mid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "rtMid" :
            rt_mid = words.split(":")[1]

        if words.split(":")[0] == "uid" :
            uids = words.split(":")[1].split("\t")
            for i in uids :
                the_uid.append(i.split("$")[0])

        if words.split(":")[0] == "rtUid" :
            rt_uid = words.split(":")[1].split("\t")[0].split("$")[0]

        if words.split(":")[0] == "rtTime" :
            rt_time = words[7 :]
    #yield (the_uid[0], (rt_mid, the_time))
    if rt_uid != None and rt_time !=None :
        if len(the_uid) > 1 :
            t1 = datetime.strptime(rt_time, "%Y-%m-%d %H:%M:%S")
            t2 = datetime.strptime(the_time, "%Y-%m-%d %H:%M:%S")
            dt = (t2-t1)/len(the_uid)
            for i in range(0, len(the_uid)-2) :
                tmp_time = datetime.strftime((t2-dt*i), "%Y-%m-%d %H:%M:%S")
                #tmp_str = "%s->%s->%s" % (the_uid[i+1], rt_mid, the_uid[i])
                tmp_str = "%s->%s" % (the_uid[i+1], rt_mid)
                yield (tmp_str, tmp_time)
        #tmp_str = "%s->%s->%s" % (rt_uid, rt_mid, the_uid[-1])
        #yield (tmp_str, rt_time)
        tmp_str = "%s->%s" % (rt_uid, rt_mid)
        yield (tmp_str, rt_time)


def resplit_libsvm(line) :
    #    0    1      2     3      4   5   6   7   8   9  10  11  12
    # "uid->mid->rtuid->time->hours->cT->cU->sI->sF->sP->sT->tP>dif"
    words = line.split("->")
    uid = words[0]
    mid = words[1]
    rt_uid = words[2]

    #
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
    tmp_str = "%s\t1:%s\t2:%s\t3:%s\t4:%s\t5:%s\t6:%s\t7:%s\t8:%s" % \
            (dif, sP, sT, sI, sF, cT, cU, tP, tD)
    tmp_id = "%s->%s" % (uid, mid)
    yield (tmp_id, tmp_str)

def resplit_labelFeatures(words, compare_time) :
    tmp_id = words[0]
    tmp_time = words[1][1]
    tmp_features = words[1][0]
    if tmp_time !=None :
        dif = 0
        features_list = []
        t1 = datetime.strptime(tmp_time, "%Y-%m-%d %H:%M:%S")
        t2 = datetime.strptime(compare_time, "%Y-%m-%d %H:%M:%S")
        if tmp_features !=None and t1 > t2 :
            tmp_list = tmp_features.split()
            dif = int(tmp_list[0])
            for i in range(1, len(tmp_list)) :
                features_list.append(float(tmp_list[i].split(":")[1]))
            yield (LabeledPoint(dif, features_list))


def resplit_timeEstimated(words, compare_time, formula_a, formula_b) :
    tmp_id = words[0]
    tmp_time = words[1][1]
    tmp_features = words[1][0]
    if tmp_time !=None :
        dif = 0
        features_list = []
        t1 = datetime.strptime(tmp_time, "%Y-%m-%d %H:%M:%S")
        t2 = datetime.strptime(compare_time, "%Y-%m-%d %H:%M:%S")
        if tmp_features !=None and t1 > t2:
            tP = float(tmp_features.split()[7].split(":")[1])
            tP = round(tP, 2)
            estimated_delay = tP * formula_a + formula_b
            t_delta = timedelta(hours=estimated_delay)
            t3 = t1 + t_delta
            time_estimated = datetime.strftime(t3, "%Y-%m-%d %H:%M:%S")
            tmp_str = "%s->%s" % (tmp_time, time_estimated)
            yield (tmp_str)

def resplit_predicted_timedelay(words) :
    dif = words[1]
    tmp_time = words[0]
    real_time = tmp_time.split("->")[0]
    predicted_time = tmp_time.split("->")[1]
    t1 = datetime.strptime(predicted_time, "%Y-%m-%d %H:%M:%S")
    tmp_str = "%d-%d" % (t1.month, t1.day)
    if dif == "1" :
        yield (tmp_time, 1)

def split_real_time(line):
    new_line = line.split("|#|")
    for words in new_line :
        if words.split(":")[0] == "time" :
            the_time = words[5 :]
            t1 = datetime.strptime(the_time, "%Y-%m-%d %H:%M:%S")
            tmp_str = "%d-%d" % (t1.month, t1.day)
            yield (tmp_str, 1)


def split_users(line):
    new_line = line.split("'")
    yield (new_line[1], 1)
    yield (new_line[3], 1)


def split_start(word):
    if word[1][1] != None :
        yield (word[1][1])

def reduce_cunc(a, b):
    if a!=None and b!=None :
        return a+b

def reduce_time(a, b):
    if a!=None and b!=None :
        t1 = datetime.strptime(a, "%Y-%m-%d %H:%M:%S")
        t2 = datetime.strptime(b, "%Y-%m-%d %H:%M:%S")
        if t1 > t2 :
            return b
        else :
            return a

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
    features_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/features_allin1"
    binladen_model_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/Model_trained_tmp"
    #tmp_out just for test
    output_tmp_path = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/binladen_tmp_out"

    #output paths
    output_real_daycount = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/binladen_real_day_count"
    output_predicted_daycount = "hdfs://node06:9000/user/function/mb_analysis/0405_analysis/binladen_predicted_day_count"
    # set the compare time
    binladen_time = "2011-05-02 23:59:59"
    # the binladen data timedelay formula:
    #   the formula is: Y = -4.19*X + 4.53, from data: delay_libsvm_0405.txt, plot in: Plot_delay.png.
    formula_a = -4.19
    formula_b = 4.53
    tweets_file = sc.textFile(tweets_path)
    features_file = sc.textFile(features_path)
    rdd_retweets = tweets_file.flatMap(lambda line: split_retweets(line))\
                   .reduceByKey(lambda a,b: reduce_time(a,b))
    # test output
    save_rdd = rdd_retweets.coalesce(1)
    save_rdd.saveAsTextFile(output_tmp_path)
    # save one finished

    #######################
    # save the real day count
    rdd_real_day_count = tweets_file.flatMap(lambda line: split_real_time(line))
    real_total_count = rdd_real_day_count.count()

    save_rdd = rdd_real_day_count.reduceByKey(lambda a,b: reduce_cunc(a,b))\
            .coalesce(1)
    save_rdd.saveAsTextFile(output_real_daycount)
    tmp_str = "\nNow the real total count is %s" % (real_total_count)
    log_write(tmp_str)
    # save one finished
    ######################

    rdd_features = features_file.flatMap(lambda line: resplit_libsvm(line))

    rdd_sub_features = rdd_features.leftOuterJoin(rdd_retweets)

    rdd_labelFeatures = rdd_sub_features.flatMap(lambda words: resplit_labelFeatures(words, binladen_time))

    rdd_time_estimated = rdd_sub_features\
            .flatMap(lambda words: resplit_timeEstimated(words, binladen_time,
                                           formula_a, formula_b))

    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    #stop_rdd = rdd_tweets.coalesce(1)
    #stop_rdd.saveAsTextFile(output_path)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"



    #Here is the trainning steps.
    binladen_model = DecisionTreeModel.load(sc, binladen_model_path)
    #
    #training_data = MLUtils.loadLibSVMFile(sc, training_path)
    test_data = rdd_labelFeatures
    # Evaluate model on test instances and compute test error
    predictions = binladen_model.predict(test_data.map(lambda x: x.features))
    # test the error value
    labelsAndPredictions = test_data.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(test_data.count())
    tmp_str = 'Test Error = ' + str(testErr)
    print(tmp_str)
    log_write(tmp_str)
    print "\n\n"

    ## Here start the time delay zip
    rdd_time_predicted = rdd_time_estimated.zip(predictions)\
            .flatMap(lambda words: resplit_predicted_timedelay(words))
    estimated_total_count = rdd_time_predicted.count()
    save_rdd = rdd_time_predicted.reduceByKey(lambda a,b: reduce_cunc(a,b))\
            .coalesce(1)
    save_rdd.saveAsTextFile(output_predicted_daycount)
    tmp_str = "\nNow the estimated total count is %s" % (estimated_total_count)
    log_write(tmp_str)
    # the save finished
