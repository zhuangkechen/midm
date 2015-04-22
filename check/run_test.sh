# get tweets
hadoop fs -rmr mb_analysis/0405_analysis/binladen_real_day_count
hadoop fs -rmr mb_analysis/0405_analysis/binladen_tmp_out
hadoop fs -rmr mb_analysis/0405_analysis/binladen_test
hadoop fs -rmr mb_analysis/0405_analysis/binladen_predicted_day_count
pyspark ~/kechen/mb_codes/spark_analysis/check/sub_binladen_retweets_list.py

