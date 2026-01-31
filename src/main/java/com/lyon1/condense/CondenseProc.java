package com.lyon1.condense;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

public class CondenseProc {

    @Context public GraphDatabaseService db;
    @Context public Log log;

    public static class ResultRow {
        public String candidate;
        public double mdlScore;
        public long superNodes;
        public long superEdges;
        public double compressionRatio;

        public ResultRow(String candidate, double mdlScore, long superNodes, long superEdges, double compressionRatio) {
            this.candidate = candidate;
            this.mdlScore = mdlScore;
            this.superNodes = superNodes;
            this.superEdges = superEdges;
            this.compressionRatio = compressionRatio;
        }
    }

    @Procedure(name = "condense.run", mode = Mode.WRITE)
    @Description("Try candidates (stars/wcc/louvain), score them, keep best, optionally write best supergraph. " +
            "For wcc/louvain you must provide a pre-projected GDS graphName.")
    public Stream<ResultRow> run(
            @Name("graphName") String graphName,
            @Name(value = "config", defaultValue = "{}") Map<String,Object> config
    ) {
        String runId = UUID.randomUUID().toString();

        @SuppressWarnings("unchecked")
        List<String> candidates = (List<String>) config.getOrDefault(
                "candidates", Arrays.asList("stars","wcc","louvain")
        );

        long degreeThreshold = ((Number) config.getOrDefault("degreeThreshold", 15)).longValue();
        boolean write = (boolean) config.getOrDefault("write", Boolean.TRUE);

        // Build each candidate, compute a simple MDL-like score, track sizes
        Map<String, Double> scoreByCand = new LinkedHashMap<>();
        Map<String, long[]> sizeByCand  = new LinkedHashMap<>();

        // Optional: if you want to drop the GDS graph at the end
        boolean dropGraph = (boolean) config.getOrDefault("dropGraph", Boolean.FALSE);

        for (String cand : candidates) {
            // cleanup any previous artifacts for this candidate+run
            cleanupCandidate(runId, cand);

            try {
                switch (cand) {
                    case "stars" -> buildStars(runId, degreeThreshold);
                    case "wcc"   -> buildWcc(runId, graphName);
                    case "louvain" -> buildLouvain(runId, graphName);
                    default -> {
                        log.warn("Unknown candidate: " + cand);
                        continue;
                    }
                }

                long superNodes = count(
                        "MATCH (s:SuperNode {runId:$runId, candidate:$cand}) RETURN count(s) AS c",
                        Map.of("runId", runId, "cand", cand)
                );
                long superEdges = count(
                        "MATCH ()-[e:SUPER_EDGE {runId:$runId, candidate:$cand}]->() RETURN count(e) AS c",
                        Map.of("runId", runId, "cand", cand)
                );

                // Simple score: fewer objects = better (you can replace with your MDL formula)
                double score = superNodes + superEdges;

                scoreByCand.put(cand, score);
                sizeByCand.put(cand, new long[]{superNodes, superEdges});

            } catch (Exception ex) {
                log.error("Candidate failed: " + cand + " runId=" + runId + " error=" + ex.getMessage(), ex);
                // Mark as worst
                scoreByCand.put(cand, Double.POSITIVE_INFINITY);
                sizeByCand.put(cand, new long[]{0,0});
            }
        }

        // Pick best (lowest score)
        String best = scoreByCand.entrySet().stream()
                .min(Comparator.comparingDouble(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .orElse(null);

        if (best == null) {
            return Stream.empty();
        }

        // Keep only best if write=true
        if (write) {
            cleanupKeepBest(runId, best);
        } else {
            // If not writing, remove everything for cleanliness
            cleanupKeepBest(runId, "__none__");
        }

        if (dropGraph) {
            safeDropGraph(graphName);
        }

        long originalNodes = count("MATCH (n) RETURN count(n) AS c", Map.of());
        long originalEdges = count("MATCH ()-[r]->() RETURN count(r) AS c", Map.of());

        String finalBest = best;
        return candidates.stream().map(c -> {
            long[] se = sizeByCand.getOrDefault(c, new long[]{0,0});
            double ratio = (originalNodes + originalEdges) == 0 ? 1.0 :
                    ((double)(se[0] + se[1])) / (double)(originalNodes + originalEdges);
            return new ResultRow(c, scoreByCand.getOrDefault(c, Double.POSITIVE_INFINITY), se[0], se[1], ratio);
        });
    }

    // -------- Candidate builders --------

    private void buildWcc(String runId, String graphName) {
        // 1) Run GDS WCC on an existing projected graph
        String prop = "cand_wcc_" + shortId(runId);

        db.executeTransactionally(
                "CALL gds.wcc.write($g, {writeProperty: $p})",
                Map.of("g", graphName, "p", prop)
        );

        // 2) Build SuperNodes from written property
        buildSuperNodesFromProperty(runId, "wcc", prop);

        // 3) Build SuperEdges between SuperNodes
        createSuperEdges(runId, "wcc");

        // Optional: remove temp property
        db.executeTransactionally(
                "MATCH (n) WHERE n[$p] IS NOT NULL REMOVE n[$p]",
                Map.of("p", prop)
        );
    }

    private void buildLouvain(String runId, String graphName) {
        String prop = "cand_louvain_" + shortId(runId);

        db.executeTransactionally(
                "CALL gds.louvain.write($g, {writeProperty: $p})",
                Map.of("g", graphName, "p", prop)
        );

        buildSuperNodesFromProperty(runId, "louvain", prop);
        createSuperEdges(runId, "louvain");

        db.executeTransactionally(
                "MATCH (n) WHERE n[$p] IS NOT NULL REMOVE n[$p]",
                Map.of("p", prop)
        );
    }

    private void buildStars(String runId, long degreeThreshold) {
        // Works on store graph directly (no GDS needed)
        db.executeTransactionally("""
            MATCH (h)
            WITH h, COUNT { (h)--() } AS deg
            WHERE deg > $thr
            CREATE (s:SuperNode {runId:$runId, candidate:'stars', groupId:elementId(h), size:deg+1})
            MERGE (h)-[:IN_SUPER {runId:$runId, candidate:'stars'}]->(s)
            WITH h, s
            MATCH (h)--(n)
            MERGE (n)-[:IN_SUPER {runId:$runId, candidate:'stars'}]->(s)
            """, Map.of("runId", runId, "thr", degreeThreshold));

        createSuperEdges(runId, "stars");
    }

    // -------- Shared helpers --------

    private void buildSuperNodesFromProperty(String runId, String cand, String prop) {
        db.executeTransactionally("""
            MATCH (n)
            WITH n[$prop] AS gid, collect(n) AS nodes
            WHERE gid IS NOT NULL
            CREATE (s:SuperNode {runId:$runId, candidate:$cand, groupId:toString(gid), size:size(nodes)})
            WITH s, nodes
            UNWIND nodes AS n
            MERGE (n)-[:IN_SUPER {runId:$runId, candidate:$cand}]->(s)
            """, Map.of("runId", runId, "cand", cand, "prop", prop));
    }

    private void createSuperEdges(String runId, String cand) {
        db.executeTransactionally("""
            MATCH (a)-[r]->(b)
            MATCH (a)-[:IN_SUPER {runId:$runId, candidate:$cand}]->(sa)
            MATCH (b)-[:IN_SUPER {runId:$runId, candidate:$cand}]->(sb)
            WHERE sa <> sb
            MERGE (sa)-[e:SUPER_EDGE {runId:$runId, candidate:$cand}]->(sb)
            ON CREATE SET e.weight = 1
            ON MATCH  SET e.weight = e.weight + 1
            """, Map.of("runId", runId, "cand", cand));
    }

    private void cleanupCandidate(String runId, String cand) {
        db.executeTransactionally("""
            MATCH (s:SuperNode {runId:$runId, candidate:$cand})
            DETACH DELETE s
            """, Map.of("runId", runId, "cand", cand));
    }

    private void cleanupKeepBest(String runId, String best) {
        // delete all SuperNodes / SUPER_EDGE for runId except best
        db.executeTransactionally("""
            MATCH (s:SuperNode {runId:$runId})
            WHERE s.candidate <> $best
            DETACH DELETE s
            """, Map.of("runId", runId, "best", best));

        db.executeTransactionally("""
            MATCH ()-[e:SUPER_EDGE {runId:$runId}]->()
            WHERE e.candidate <> $best
            DELETE e
            """, Map.of("runId", runId, "best", best));
    }

    private long count(String cypher, Map<String,Object> params) {
        return (long) db.executeTransactionally(cypher, params, r -> (long) r.next().get("c"));
    }

    private String shortId(String runId) {
        return runId.replace("-", "").substring(0, 8);
    }

    private void safeDropGraph(String graphName) {
        try {
            db.executeTransactionally("CALL gds.graph.drop($g)", Map.of("g", graphName));
        } catch (Exception e) {
            log.warn("Could not drop GDS graph '" + graphName + "': " + e.getMessage());
        }
    }
}
