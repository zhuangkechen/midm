# get tweets
#pyspark ~/kechen/mb_codes/spark_analysis/net_2/get_retweet_links.py
#pyspark ~/kechen/mb_codes/spark_analysis/net_2/sub_tweets_network.py
pyspark ~/kechen/mb_codes/spark_analysis/get_2011_tweets.py
#
pyspark ~/kechen/mb_codes/spark_analysis/net_2/get_retweet_period.py
pyspark ~/kechen/mb_codes/spark_analysis/net_2/get_user_follow_tweet.py
pyspark ~/kechen/mb_codes/spark_analysis/net_2/get_mutual.py
pyspark ~/kechen/mb_codes/spark_analysis/net_2/get_retweet_counts.py
pyspark ~/kechen/mb_codes/spark_analysis/net_2/get_retweet_trees_with_time.py
pyspark ~/kechen/mb_codes/spark_analysis/net_2/get_mutual_with_retweet.py
# the libsvm
pyspark ~/kechen/mb_codes/spark_analysis/net_2/feature_merge.py
pyspark ~/kechen/mb_codes/spark_analysis/net_2/convert_libsvm.py
# the model
pyspark ~/kechen/mb_codes/spark_analysis/net_2/gad_feature_DTM.py

# divide
# get tweets
pyspark ~/kechen/mb_codes/spark_analysis/net_sub/divide_test.py
pyspark ~/kechen/mb_codes/spark_analysis/net_sub/divide_training.py
# the model
pyspark ~/kechen/mb_codes/spark_analysis/net_sub/real_DTM.py
