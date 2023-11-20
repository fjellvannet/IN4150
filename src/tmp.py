import networkx as nx
import random
import matplotlib.pyplot as plt

def generate_connected_network(n, k):
    if k >= n:
        raise ValueError("k should be less than n for a connected network.")

    G = nx.Graph()

    # Add nodes to the graph
    G.add_nodes_from(range(n))

    # Connect each node to k other nodes
    for node in range(n):
        #connected_nodes = random.sample(list(set(range(n)) - {node}), k)
        connected_nodes = (e % n for e in (node % i for i in range(2, k + 2)))#node - 1, node + 1, (node + n/2)))
        connected_nodes = (e % n for e in (node - 1, node + 1, *(int(node + j * n / (k - 1)) for j in range(1, k - 1))))




        G.add_edges_from([(node, other_node) for other_node in connected_nodes])

    return G

# Example usage
n = 12  # Number of nodes
k = 4   # Number of connections per node

graph = generate_connected_network(n, k)

# Print the edges of the graph
print("Edges:")
for edge in graph.edges():
    print(edge)

# Visualize the graph
pos = nx.circular_layout(graph)
nx.draw(graph, pos, with_labels=True, font_weight='bold', node_size=700, node_color='skyblue', font_size=8, font_color='black', edge_color='gray', linewidths=1, alpha=0.7)
plt.show()