import sys

from pyspark import SparkContext, SparkConf

def split_func(line):
    new_line = line.split("'")
    yield (new_line[1], 1)

def split_users(line):
    new_line = line.split()
    yield (new_line[0], new_line[1])

def split_start(word):
    if word[1][1] != None :
        yield (word[1][0], ("X", 0))
        yield (word[0], (word[1][0], 1))
        #yield (word[1][0], ("X", 1))
    else :
        yield (word[0], (word[1][0], 0))

def re_split(word):
    infected = False
    for i in word[1] :
        if i[0] == "X" :
            infected = True
            break
    if infected :
        for i in word[1] :
            if i[0] == "X" :
                continue
            elif i[1] == 1 :
                yield (word[0], (i[0], i[1]))
            elif int(i[0][-1]) % 4 != 0 :
                yield (word[0], (i[0], i[1]))
            else :
                yield (word[0], (i[0], 1))
                yield (i[0], ("X", 0))
    else :
        for i in word[1] :
            yield (word[0], (i[0],i[1]))

def map_func(word):
    if word[1][1] == 1 :
        yield (word[0], word[1][0])

def map_count(word):
    yield (word[0], 1)
    yield (word[1], 1)

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
    rawPath = "hdfs://node06:9000/user/function/rawdata/finalsn"
    input_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/large_topic_users_count_20_and_1"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_tmp2"
    output_path_10 = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_tmp_10"
    output_path_5 = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_tmp_5"
    output_path_30000 = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_tmp_30000"
    output_path_50000 = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_tmp_50000"
    output_path_100000 = "hdfs://node06:9000/user/function/mb_analysis/gaddafi_analysis/network_tmp_100000"
    in_file = sc.textFile(input_path)
    raw_file = sc.textFile(rawPath)
    choosen_users = in_file.flatMap(lambda line: split_func(line))\
            .reduceByKey(lambda a,b: reduce_cunc(a,b))
    raw_users = raw_file.flatMap(lambda line: split_users(line))
    all_users =  raw_users.leftOuterJoin(choosen_users)\
            .flatMap(lambda line: split_start(line))

    # Here start the loop
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    loop = all_users
    if_30000 = False
    if_50000 = False
    if_100000 = False
    current_count = 0
    for i in range(0, 20):
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
        print "This is the %d-th loop, let's wait..................\n" % (i+1)
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
        print "\n\n"
        loop = loop.groupByKey().flatMap(lambda word: re_split(word))
        stop_rdd = loop.flatMap(lambda word: map_func(word))\
                .coalesce(1)
        count_rdd = stop_rdd.flatMap(lambda word: map_count(word))\
                .reduceByKey(lambda a, b: reduce_cunc(a,b))\
                .coalesce(1)
        the_count = count_rdd.count()
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
        tmp_s =  "This is the %d-th loop, and the network now have %d users!!!!!!!!!!!!!!!!!!\n" % (i+1, the_count)
        print tmp_s
        log_write(tmp_s)
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
        print "\n\n"
        if the_count == current_count :
            print "***************************************************\n"
            tmp_s =  "***  The count not increase now, BREAKING NOW ****\n"
            print tmp_s
            log_write(tmp_s)
            print "***************************************************\n"
            break
        else :
            current_count = the_count
        if the_count > 30000 and if_30000 == False :
            stop_rdd.saveAsTextFile(output_path_30000)
            if_30000 = True
        if the_count > 50000 and if_50000 == False :
            stop_rdd.saveAsTextFile(output_path_50000)
            if_50000 = True
        if the_count > 100000 and if_100000 == False :
            stop_rdd.saveAsTextFile(output_path_100000)
            if_100000 = True
        if i == 4 :
            stop_rdd.saveAsTextFile(output_path_5)
        if i == 9 :
            stop_rdd.saveAsTextFile(output_path_10)


    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"

    stop_rdd = loop.flatMap(lambda word: map_func(word)).coalesce(1)
    stop_rdd.saveAsTextFile(output_path)
