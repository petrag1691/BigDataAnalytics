#!/usr/bin/env python

import sys
INTERMEDIATE_FILENAME = '/home/cloudera/clusters.txt'
first_time = 1
clusters = dict()
allClusterPoints = dict()
centroids = dict()
index = 0

def writeToIntermediateFile():
	FILE = open(INTERMEDIATE_FILENAME,'w')
	FILE.truncate()
	FILE.seek(0)
	for clusterID, coords in centroids.iteritems():
		message = str(clusterID) + '\t'
		for i in range(10):
			message += str(coords[i])+','
		message = message[:-1] + '\n'
		FILE.write(message)
	FILE.close()

def getCentroids(coords):
	centroidsCoords = list()
	for pts in zip(*coords):
		centroidsCoords.append(sum(pts)/len(pts))
	return centroidsCoords

def calculateCentroids():
	for clusterID, coords in allClusterPoints.iteritems():
		centroids[clusterID] = getCentroids(coords)
	writeToIntermediateFile()

# MAIN CODE
for line in sys.stdin:
	line = line.strip()
	clusterID, point = line.split('\t')
	pointID, pointCoord = point.split(';')
	clusters[index] = (clusterID, pointID)
	index = index + 1
	if not (allClusterPoints.has_key(clusterID)):
		allClusterPoints[clusterID] = list()
	pointCoord = map(float,pointCoord.split(','))
	allClusterPoints[clusterID].append(pointCoord)

calculateCentroids()

# Print the Output
for key in clusters:
	clusterID, pointID = clusters[key]
	if first_time == 1:
		prevClusterID = clusterID
		first_time = 0
		nextCluster = 2
		message = "Cluster1\t"
	if clusterID == prevClusterID:
		message += pointID + ','
	else:
		message = message[:-1]
		print message
		prevClusterID = clusterID
		message = 'Cluster'+ str(nextCluster) + '\t'
		nextCluster = nextCluster + 1
# Last Cluster
message = message[:-1]
print message
