import sys

from pyspark import SparkContext, SparkConf
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils

def log_write(counts):
    f = open("model.txt", 'a')
    f.write(counts)
    f.close()
if __name__ == "__main__":
    appName = "Kechen_test_DecisionTreeModel"
    master = "spark://node06:7077"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)

    feature_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/features_libsvm"

    model_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/Model_test"


    features_data = MLUtils.loadLibSVMFile(sc, feature_path)
    model = DecisionTree.trainClassifier(features_data, numClasses=2, categoricalFeaturesInfo={},\
                                        impurity='gini', maxDepth=9, maxBins=32)

    tmp_str = model.toDebugString()
    print(tmp_str)
    log_write(tmp_str)
    model.save(sc, model_path)


    # Here start the job
    print "######################################################\n"
    print "######################################################\n"
    print "#########            Start!!!                  #######\n"
    print "######################################################\n"
    print "######################################################\n"
    print "\n\n\n"
    print "****************************************************\n"
    print "Here is the last step\n"
    print "****************************************************\n"
    print "\n\n"
