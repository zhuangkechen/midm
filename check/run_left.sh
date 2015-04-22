#hadoop fs -mkdir mb_analysis/new_network_analysis
#hadoop fs -cp mb_analysis/0910_analysis/network_mutual mb_analysis/new_network_analysis/
#hadoop fs -cp mb_analysis/0910_analysis/network_tmp2 mb_analysis/new_network_analysis/
#hadoop fs -cp mb_analysis/0910_analysis/retweet_periods_2011 mb_analysis/new_network_analysis/
#hadoop fs -cp mb_analysis/0910_analysis/tweets_tmp2 mb_analysis/new_network_analysis/
#hadoop fs -cp mb_analysis/0910_analysis/user_follow_tweet_period mb_analysis/new_network_analysis/
#hadoop fs -cp mb_analysis/0910_analysis/user_retweet_2011_from mb_analysis/new_network_analysis/
#hadoop fs -cp mb_analysis/0910_analysis/user_retweet_count_with_mutual mb_analysis/new_network_analysis/
# get tweets

#pyspark ~/kechen/mb_codes/spark_analysis/net_2/merge_tree.py
#pyspark ~/kechen/mb_codes/spark_analysis/net_2/feature_merge.py
#hadoop fs -mv mb_analysis/new_network_analysis/features_allin1 mb_analysis/new_network_analysis/features_allin2
#pyspark ~/kechen/mb_codes/spark_analysis/net_2/merge_features.py
#pyspark ~/kechen/mb_codes/spark_analysis/net_2/convert_libsvm.py

# divide
# get tweets
pyspark ~/kechen/mb_codes/spark_analysis/net_sub/divide_test.py
pyspark ~/kechen/mb_codes/spark_analysis/net_sub/divide_training.py
# the model
pyspark ~/kechen/mb_codes/spark_analysis/net_sub/real_DTM.py
#pyspark ~/kechen/mb_codes/spark_analysis/net_2/corr_libsvm.py
