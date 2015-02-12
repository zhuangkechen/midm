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
    for words in new_line:
        tags = words.split(":")
        tag = tags[0]
        if tag == "uid":
            uid = tags[1].split()
            the_uid = uid[0].split("$")
            return (the_uid[0], 1)


def reduce_func(a, b):
    if a!=0 and b!=0 :
        return a+b

if __name__ == "__main__":
    appName = "Kechen_userTweetsCount"
    master = "spark://node06:7077"
    rawPath = "hdfs://node06:9000/user/function/rawdata/microblogs/sortedmicroblogs.txt"
    inputPath = "hdfs://node06:9000/user/function/mb_analysis/network"
    outputPath = "hdfs://node06:9000/user/function/mb_analysis/spark_analysis/tweetsCount"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    file = sc.textFile(rawPath)
    counts = file.map(lambda line: split_func(line)) \
            .reduceByKey(lambda a,b: reduce_func(a,b))\
            .coalesce(1)
    printCount = counts.count()
    totalTweets = counts.mapValues(lambda value: value)\
            .reduce(lambda a,b: reduce_func(a,b))\
            .collect()
    for i in totalTweets:
        print "final total count:%d" % (i)
    print "uids count:%d" % (printCount)
    counts.saveAsTextFile(outputPath)
