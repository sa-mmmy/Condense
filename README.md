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

# How it Works : 

clone the repository 

build the package 

you will see condense.0.0.1.jar in the target directory 

add the jar in the directory of /plugin of your Neo4j instance 

modify config.txt of your instance to include the recognize the condense pluging 

dbms.security.procedures.unrestricted=gds.*, condense.* 

dbms.security.procedures.allowlist=gds.*, condense.*


create ypur graph in the instance 

## 1. Project a graph into GDS (required for WCC / Louvain) :
   
CALL gds.graph.project(

  'myGraph',
  
  'Person',
  
  {FOLLOWS: {orientation: 'UNDIRECTED'}}
  
);

## 2. Run the condensation procedure :
   
CALL condense.run('myGraph', {

  candidates: ['stars','wcc','louvain'],
  
  degreeThreshold: 10,
  
  write: true
  
})

YIELD candidate, mdlScore, superNodes, superEdges, compressionRatio

RETURN *

ORDER BY mdlScore ASC;


## 3. Compare results 
