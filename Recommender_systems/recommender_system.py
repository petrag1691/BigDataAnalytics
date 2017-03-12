from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
from sklearn.metrics.pairwise import cosine_similarity
from math import sqrt
import re
import numpy as np

class Recommender(MRJob):
	def steps(self):
		return [
		MRStep(mapper=self.makeUserKey,reducer=self.makeInvertedIndex)
		,MRStep(mapper=self.cooccurrence, reducer=self.similarity)
		]
	def makeUserKey(self, _, line):
		row_list = line.split()
		if len(row_list)==4:
			yield row_list[0],(row_list[1],row_list[2])
	
	def makeInvertedIndex(self,user_id,movie_rating):
		#print user_id
		movie_rating_list = []
		for movie,rating in movie_rating:
			movie_rating_list.append((movie,rating))
		#print user_id,movie_rating_list
		yield user_id,movie_rating_list	
	
	def cooccurrence(self,user_id,agg_movie_rating):
		#print 'This is the user::::::::::::::::::::::::::::::::::::::::::::::::',user_id,len(agg_movie_rating)
		counter =0;
		for i,movie_pair_root in enumerate(agg_movie_rating,start=0):
			#print i;
			if i<len(agg_movie_rating):
				k = i+1;
				movie_id_root = movie_pair_root[0]
				movie_rating_root = movie_pair_root[1]
				for j,movie_pair_follow in enumerate(agg_movie_rating[k:],start=k):
					#print i,j;
					counter = counter+1
					movie_id_follow = movie_pair_follow[0]
					movie_rating_follow = movie_pair_follow[1]
					#print movie_id_root,movie_id_follow,movie_rating_root,movie_rating_follow
					yield (movie_id_root,movie_id_follow),(movie_rating_root,movie_rating_follow)	
		#print 'counter is ::::::::::::',counter
	def similarity(self,movie_pair,rating_pair):
		#print movie_pair
		rating_pair1 = []
		rating_pair2 = []
		for rating_1,rating_2 in rating_pair:
			rating_pair1.append(rating_1)
			rating_pair2.append(rating_2)
		#similarity
		mean_1 =reduce(lambda x, y: x + y, rating_pair1) / float(len(rating_pair1))
		mean_2 =reduce(lambda x, y: x + y, rating_pair2) / float(len(rating_pair2))
		rating_pair1[:] = [x - mean_1 for x in rating_pair1]
		rating_pair2[:] = [x - mean_2 for x in rating_pair2]
		sxx = syy = sxy=0
		rating_pair_final = zip(rating_pair1,rating_pair2)
		for ratingx,ratingy in rating_pair_final:
			sxx = sxx+ratingx*ratingx
			syy = syy+ratingy*ratingy
			sxy = sxy+ratingx*ratingy
		numerator = sxy
		denominator = sqrt(sxx)*sqrt(syy)
		similarity = numerator/float(denominator)
		#alternate method to calculate similarity
		#similarity = cosine_similarity(np.array(rating_pair1),np.array(rating_pair12))
		#print similarity
		yield movie_pair,similarity
		
if __name__ == '__main__':
    Recommender.run()
			
			
		