# API: python generate.py <path of output file> <number of nodes> <probability of edges> <probability of fraud>
# <probability of edges> is defined as the number of nodes that will be linked to this node / the number of all pre-existing nodes

import sys
import random

if __name__ == '__main__':
	output_path = sys.argv[1]
	num_nodes = int(sys.argv[2])
	prob_edges = float(sys.argv[3])
	prob_fraud = float(sys.argv[4])

	f = open(output_path, 'w')
	for i in range(1, num_nodes+1):
		fraud_flg = True if random.random() <= prob_fraud else False
		neighbors = list()
		for j in range(1, i):
			if random.random() <= prob_edges:
				neighbors.append(str(j))
		f.write('%d|%s|%s\n' % (i, fraud_flg, ','.join(neighbors)))

		if i % 10000 == 0:
			print i
