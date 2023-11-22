import itertools
import sys
import random


def generate_connections(n, k):
    if k >= n:
        raise ValueError("k should be less than n for a connected network.")
    connections = [[] for _ in range(n)]
    for node in range(n):  # Iterate through nodes
        available_nodes = tuple(
            id_  # only keep the node's id, available_nodes doesn't need a reference to each node's connections
            for id_, conns in sorted(  # sort nodes by the number of connections they already have - fewest first
                # get nodes > current node with their connections in random order
                random.sample(tuple(enumerate(connections[node + 1 :], node + 1)), n - node - 1),
                # tuple(enumerate(connections[node + 1:], node + 1)),  # keep this line to turn off random if you prefer
                key=lambda x: len(x[1]),  # stable sorting -> the nodes with the same number of conns are still random
            )
            if len(conns) < k  # filter out nodes that are already fully connected to k other nodes
        )
        if len(available_nodes) < k - len(connections[node]):  # fewer nodes available than needed - add extra edge(s)
            new_conns = (
                *available_nodes,  # connect to all the available nodes to add as few extra edges as possible
                *tuple(  # to connect this node to at least k others, add extra edges to already fully connected nodes
                    y
                    for y, _ in sorted(  # sort nodes by the number of connections they already have - fewest first
                        random.sample(  # get nodes not being the current node and not already connected in random order
                            tuple(
                                x
                                for x in enumerate(connections)  # get node-id's with their connections
                                if x[0] not in (*available_nodes, node, *connections[node])
                            ),
                            n - len(available_nodes) - len(connections[node]) - 1,
                        ),
                        key=lambda x: len(x[1]),  # stable sorting -> nodes with the same number of conns still random
                    )
                )[
                    : k - len(available_nodes) - len(connections[node])
                ],  # only keep necessary number of conn candidates
            )
        elif available_nodes and k - len(connections[node]) > 0:  # normal case - enough nodes available for connection
            new_conns = available_nodes[: k - len(connections[node])]  # connect to necessary number of available nodes
        else:  # the node is already fully connected as other nodes already have connected to it, occurs towards end
            continue
        connections[node].extend(new_conns)
        for i in new_conns:  # connections are bidirectional - add this node to the other nodes it connects to
            connections[i].append(node)
    # If two nodes have more than k edges and are connected, remove edge between them. Rarely happens
    for nodes in itertools.combinations((index for index, nodes_ in enumerate(connections) if len(nodes_) > k), 2):
        if all(x in connections[x] and len(connections[x]) > k for x in nodes):
            for node_1, node_2 in (nodes, reversed(nodes)):  # Remove edge in both nodes / both directions
                connections[node_1].remove(node_2)
    return connections


def get_optimal_connections(n, k):
    optimal_conns = None
    conn_num = sys.maxsize
    seed = 0
    i = 0
    for i in range(10):  # Try up to 10 times to find a solutions without additional edges
        random.seed(i)
        connections = generate_connections(n, k)
        conn_num = sum(len(conns) - k for conns in connections)  # conn_num is the number of additional edges
        if optimal_conns is None or conn_num < sum(len(c) - k for c in optimal_conns):  # first run or better conn_num
            optimal_conns = connections  # only update if connections actually is better (lower conn_num) than before
            seed = i
            if not conn_num:  # Immediately leave the loop if there are no additional edge, mostly occurs in first loop
                break
    print(
        f"Random seed {seed}{f', tried seeds from 0 to {i} to avoid {conn_num} additional edge(s)' if conn_num else ''}"
    )
    print(f"Connections: {dict(enumerate(optimal_conns))}")
    return optimal_conns
