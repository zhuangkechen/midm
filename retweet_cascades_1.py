import sys

from pyspark import SparkContext, SparkConf


def degreeScore(scores):
    rank = int(scores)/1000.0
    if rank > 1 :
        return scores, 1
    else :
        return scores, rank

def split_func(line):
    new_line = line.split("|#|")
    post_time = new_line[0][5 :]
    post_uid = []
    origin_uid = ""
    rt_uid = ""
    for words in new_line:
        tags = words.split(":")
        tag = tags[0]
        if tag == "uid":
            uids = tags[1].split()
            for uid in uids:
                the_uid = uid.split("$")
                post_uid.append(the_uid[0])
        elif tag == "rtUid" :
            uids = tags[1].split()
            the_uid = uids[0].split("$")
            rt_uid = the_uid[0]
    if rt_uid == "" and post_uid =="942492951224838":
        tmpstr = "%s->%s" % ("null", post_uid[0])
        yield(tmpstr, post_time)
    else :
        if rt_uid != "942492951224838" :
            return
        else :
            if len(post_uid) == 1 :
                tmpstr = "%s->%s" % (rt_uid, post_uid[-1])
                yield (tmpstr, post_time)
            if len(post_uid) > 1 :
                tmp_time = post_time
                for i in range(0, len(post_uid)-2) :
                    tmpstr = "%s->%s" % (post_uid[i+1], post_uid[i])
                    yield (tmpstr, tmp_time)
                    tmp_time = "null"
                tmpstr = "%s->%s" % (rt_uid, post_uid[-1])
                yield (tmpstr, tmp_time)


def reduce_func(a, b):
    if a != "null" :
        return a
    else :
        return b

if __name__ == "__main__":
    appName = "Kechen Build a sample retweet cascade for mid=550001852885225338"
    master = "spark://node06:7077"
    inputPath = "hdfs://node06:9000/user/function/mb_analysis/retweet_analysis/get_top_5_retweets"
    outputPath = "hdfs://node06:9000/user/function/mb_analysis/spark_analysis/sample_cascade"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    file = sc.textFile(inputPath)
    counts = file.flatMap(lambda line: split_func(line)) \
            .reduceByKey(lambda a,b: reduce_func(a,b))\
            .coalesce(1)
    counts.saveAsTextFile(outputPath)
