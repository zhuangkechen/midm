# get tweets
hadoop fs -rmr mb_analysis/0405_analysis/gaddafi_real_day_count
hadoop fs -rmr mb_analysis/0405_analysis/gaddafi_tmp_out
hadoop fs -rmr mb_analysis/0405_analysis/gaddafi_test
hadoop fs -rmr mb_analysis/0405_analysis/gaddafi_predicted_day_count
pyspark ~/kechen/mb_codes/spark_analysis/check/sub_gaddfi_retweets_list.py

