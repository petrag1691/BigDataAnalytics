#!/usr/bin/env python

import sys, re, math
INTERMEDIATE_FILENAME = '/home/cloudera/pageRank.txt'
allPageRanks = dict()
node_incomingPG = dict()
iteration = int(sys.argv[1])
initialPageRank = float(sys.argv[2])

#FUNCTIONS
def readPageRanks():
	FILE = open(INTERMEDIATE_FILENAME, 'r')
	data = FILE.read()
	FILE.close()
	del FILE
	index = 0
	for line in data.strip().split("\n"):
		nodeID, pageRank = line.strip().split("\t")
		allPageRanks[index] = (nodeID, pageRank)
		index = index + 1

def getPageRank(node):
	for key in allPageRanks:
		nodeID, pageRank = allPageRanks[key]
		nodeID = str(nodeID)
		if nodeID == node:
			return pageRank

# MAIN CODE
for line in sys.stdin:
	line = line.strip()
	data = line.split("\t")
	nodes = data[1].split(",")
	if iteration == 1:
		for node in nodes:
			pageRank = initialPageRank
			print str(node) + "\t" + str(pageRank) + "\t" + str(len(nodes))
	else:
		readPageRanks()
		for node in nodes:
			pageRank = getPageRank(data[0])
			print str(node) + "\t" + str(pageRank) + "\t" + str(len(nodes))
