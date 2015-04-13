import sys

from pyspark import SparkContext, SparkConf

def split_uid_mid(line):
    new_line = line.split("|#|")
    uids = []
    for words in new_line :
        if words.split(":")[0] == "time" :
            the_time = words.split(":")[1]
            year = the_time.split("-")[0]
            month = the_time.split("-")[1]
            if year != "2011" :
                break
            if month != "07" and month != "08" :
                break
        if words.split(":")[0] == "uid" :
            the_uid = words.split(":")[1].split("\t")
            for tmp_uid in the_uid :
                uid = tmp_uid.split("$")
                uids.append(uid[0])

        if words.split(":")[0] == "rtUid" :
            the_uid = words.split(":")[1].split("\t")
            uids.append(the_uid[0].split("$")[0])
    if len(uids) > 1 :
        for i in range(0,len(uids)-2) :
            tmp_str = "%s\t%s" % (uids[i+1], uids[i])
            yield (tmp_str, 1)

def split_start(word):
    if word[1] >= 1 :
        u1 = word[0].split()[0]
        u2 = word[0].split()[1]
        yield (u1, u2)

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

    raw_network_path = "hdfs://node06:9000/user/function/rawdata/microblogs/sortedmicroblogs.txt"
    #raw_network_path= "hdfs://node06:9000/user/function/mb_analysis/choosenTweetsGaddafi"
    #retweet_user_path = "hdfs://node06:9000/user/function/mb_analysis/all_in_retweet_user_count"
    output_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/network_tmp2"
    #output_path = "hdfs://node06:9000/user/function/mb_analysis/2011_01_10_retweet_links"

    raw_network_file = sc.textFile(raw_network_path)
    rdd_links = raw_network_file.flatMap(lambda line: split_uid_mid(line))

    rdd_result = rdd_links.reduceByKey(lambda a,b: reduce_cunc(a,b))\
            .flatMap(lambda words: split_start(words))


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
    tmp_str = "\nthe retweet links in 2011-07 to 08 retweet network  count is %d" % (the_counts)
    log_write(tmp_str)
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
