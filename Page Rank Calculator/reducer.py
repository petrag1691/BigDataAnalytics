#!/usr/bin/env python

import sys
dampingFactor = float(sys.argv[1])
nodePR = 1-dampingFactor
index = 0
first_time = 1
prev_node = 0
PR = dict()
INTERMEDIATE_FILENAME = '/home/cloudera/pageRank.txt'

#FUNCTIONS
def writeToIntermediateFile():
	FILE = open(INTERMEDIATE_FILENAME,'w+')
	FILE.truncate()
	FILE.seek(0)
	for key in PR:
		nodeID, PageRank = PR[key]
		nodeID = int(nodeID)		
		FILE.write(str(nodeID) + "\t" + str(PageRank) + '\n')
	FILE.close()

# MAIN CODE
for line in sys.stdin:
	line = line.strip()
	node, incoming_PR, countOL = line.split("\t")
	node = float(node)
	incoming_PR = float(incoming_PR)
	countOL = float(countOL)
	if first_time == 1:
		prev_node = node
		first_time = 0
	if node == prev_node:
		nodePR = nodePR + dampingFactor*(incoming_PR/countOL)
	else:
		PR[index] = (prev_node, nodePR)
		index = index + 1
		nodePR = 1-dampingFactor
		nodePR = nodePR + dampingFactor*(incoming_PR/countOL)
		prev_node = node

PR[index] = (prev_node, nodePR)

writeToIntermediateFile()

for key in PR:
	nodeID, PageRank = PR[key]
	nodeID = int(nodeID)
	print str(nodeID) + "\t" + str(PageRank)
