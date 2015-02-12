# This is just a test program for fun.
# By Kechen
# zhuangkechen@gmail.com

import sys

from pyspark import SparkContext, SparkConf


def degreeScore(scores):
    rank = int(scores)/1000.0
    if rank > 1 :
        return scores, 1
    else :
        return scores, rank

def split_func(line):
    new_line = line.split("'")
    uids = new_line[1]
    uid = uids.split("$")[0]
    counts = new_line[2].split()[1]
    count = int(counts[0])
    return (uid, count)

def map_func(word):
    uids = word[0]
    counts = word[1]
    return (word, counts)

def reduce_func(a, b):
    if a!=0 and b!=0 :
        return a+b

if __name__ == "__main__":
    appName = "Kechen_userTweetsCount"
    master = "spark://node06:7077"
    inputPath = "hdfs://node06:9000/user/function/mb_analysis/spark_analysis/tweetsCount/part-00000"
    outputPath = "hdfs://node06:9000/user/function/mb_analysis/spark_analysis/sTweetsCount"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    file = sc.textFile(inputPath)
    counts = file.map(lambda line: split_func(line)) \
            .reduceByKey(lambda a,b: reduce_func(a,b))\
            .coalesce(1)
    counts.saveAsTextFile(outputPath)
