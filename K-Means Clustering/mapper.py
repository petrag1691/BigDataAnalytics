#!/usr/bin/env python

import sys, re, math, random
INTERMEDIATE_FILENAME = '/home/cloudera/clusters.txt'
points = dict()
clusters = dict()
centroids = dict()
iteration = int(sys.argv[1])
K_Clusters = int(sys.argv[2])

#FUNCTIONS
def readClusterCentroids():
	if iteration == 1:
		coords = []
		seed = 1000
		for i in range(K_Clusters):
			seed = seed + (i*100)
			random.seed(seed)
			randomIndex = random.randint(1, 1000)
			randomIndex = str(randomIndex)
			for key in points:
				pointID, pointCoords = points[key]
				if randomIndex == pointID:
					centroids[i] = (i+1, pointCoords)
		return
	FILE = open(INTERMEDIATE_FILENAME, 'r')
	data = FILE.read()
	FILE.close()
	del FILE
	index = 0
	for line in data.strip().split("\n"):
		clusterID, coords = line.strip().split("\t")
		coords = coords.split(",")
		centroids[index] = (clusterID, coords)
		index = index + 1

def calculateDistance(pointCoords, centroidCoords):
	distance = 0
	for i in range(10):
		distance = distance + math.pow((float(pointCoords[i])-float(centroidCoords[i])),2)
	distance = math.sqrt(distance)
	return distance

def generateClustersPairs():
	index = 0
	for key in points:
		pointID, pointCoords = points[key]
		leastDistance = float("inf")
		for centroidKey in centroids:
			clusterID, centroidCoords = centroids[centroidKey]
			distance = calculateDistance(pointCoords, centroidCoords)
			if distance < leastDistance:
				leastDistance = distance
				closestCentroid = clusterID
		clusters[index] = (closestCentroid, pointID, pointCoords)
		index = index + 1


# MAIN CODE
for line in sys.stdin:
	line = line.strip()
	data = line.split("\t")
	coords = data[1].split(",")
	pointID = data[0]
	points[pointID] = (pointID, coords)

readClusterCentroids()
generateClustersPairs()

# Print the Output
for key in clusters:
	clusterID, pointID, pointCoords = clusters[key]
	message = str(clusterID) + '\t' + str(pointID) + ';'
	for i in range(10):
		message += str(pointCoords[i])+','
	message = message[:-1]
	print message
