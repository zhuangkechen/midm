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

    #training_path = "hdfs://node06:9000/user/function/mb_analysis/0910_analysis/training_libsvm"
    #test_path = "hdfs://node06:9000/user/function/mb_analysis/0910_analysis/test_libsvm"
    training_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/training_libsvm_tmp"
    test_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/test_libsvm_tmp"

    model_path = "hdfs://node06:9000/user/function/mb_analysis/new_network_analysis/Model_trained_tmp"


    training_data = MLUtils.loadLibSVMFile(sc, training_path)
    test_data = MLUtils.loadLibSVMFile(sc, test_path)
    model = DecisionTree.trainClassifier(training_data, numClasses=2, categoricalFeaturesInfo={},\
                                        impurity='gini', maxDepth=9, maxBins=32)
    # Evaluate model on test instances and compute test error
    predictions = model.predict(test_data.map(lambda x: x.features))
    labelsAndPredictions = test_data.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(test_data.count())
    tmp_str = 'Test Error = ' + str(testErr)
    print(tmp_str)
    log_write(tmp_str)
    print('Learned classification tree model:')
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
