1.The following recommender system produces a similarity between two movies in the movie lens dataset.
2.The columns of the dataset include userid,movieid,rating and time.
3.MRJob library is used to execute this Recommender system  in hadoop mapreduce.
4.Item-item collaborative filtering is used to obtain the similarity between two movies.

Instructions:
1.Local execution:
python recommender_system.py u.data
2.Hadoop execution:
python my_job.py -r hadoop hdfs://my_home/u.data
