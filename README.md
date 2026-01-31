# Project Description (detailed)

Real-world graphs often contain redundant or repetitive structures (hubs, components, communities). Choosing a single compression strategy a priori is suboptimal, as different graphs exhibit different structural properties.

This project implements an adaptive graph condensation algorithm for Neo4j. Instead of committing to one compression model, the system:

Generates multiple condensed representations of the same graph using different algorithms:

Stars (hub-based compression)

Weakly Connected Components (WCC)

Louvain community detection (via Neo4j Graph Data Science)

Evaluates each representation using an MDL-inspired score, estimating the cost of describing the compressed graph.

MDL ≈ |SuperNodes| + |SuperEdges| 

Automatically selects the best model (lowest MDL score).

Materializes the winning representation as a supergraph inside Neo4j, composed of:

:SuperNode nodes

:SUPER_EDGE relationships

:IN_SUPER membership links from original nodes

The plugin is implemented as a custom Neo4j procedure and integrates seamlessly with the Neo4j Graph Data Science (GDS) library.
