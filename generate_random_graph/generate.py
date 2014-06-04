# API: python generate.py <number of nodes> <probability of edges>

import sys
import random
import networkx as nx

if __name__ == '__main__':
	num_nodes = int(sys.argv[1])
	prob_edges = float(sys.argv[2])

	output_path = 'graph_%d_%4.2f.data' % (num_nodes, prob_edges)
	f1 = open('bfs_' + output_path, 'w')
	f2 = open('fw_' + output_path, 'w')

	G = nx.gnp_random_graph(num_nodes, prob_edges)

	for node in G.nodes():
		f1.write('%d\t%s|Integer.MAX_VALUE|WHITE|null\n' % (node, ','.join(map(lambda n: str(n), G.neighbors(node)))))

		for end_node in G.nodes():
			if end_node in G.neighbors(node):
				f2.write('%d,%d,%d\n' % (node, end_node, 1))

	f1.close()
	f2.close()
		
		

	
