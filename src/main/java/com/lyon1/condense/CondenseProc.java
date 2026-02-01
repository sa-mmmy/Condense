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
    @Description("Exécute et compare les stratégies de condensation (cliques, stars, wcc, louvain, leiden, lpa, chains, kcore) via un score MDL.")
    public Stream<ResultRow> run(
            @Name("graphName") String graphName,
            @Name(value = "config", defaultValue = "{}") Map<String,Object> config
    ) {
        String runId = UUID.randomUUID().toString();

        @SuppressWarnings("unchecked")
        List<String> candidates = (List<String>) config.getOrDefault("candidates",
                Arrays.asList("cliques", "stars", "wcc", "louvain", "leiden", "lpa", "chains", "kcore"));

        long degreeThr = ((Number) config.getOrDefault("degreeThreshold", 15)).longValue();
        int kValue = ((Number) config.getOrDefault("kValue", 3)).intValue();
        boolean write = (boolean) config.getOrDefault("write", Boolean.TRUE);

        long originalNodes = count("MATCH (n) RETURN count(n) AS c", Map.of());
        long originalEdges = count("MATCH ()-[r]->() RETURN count(r) AS c", Map.of());

        Map<String, Double> scoreByCand = new LinkedHashMap<>();
        Map<String, long[]> sizeByCand  = new LinkedHashMap<>();

        for (String cand : candidates) {
            cleanupCandidate(runId, cand);

            try {
                switch (cand.toLowerCase()) {
                    case "stars"   -> buildStars(runId, degreeThr);
                    case "wcc"     -> buildWcc(runId, graphName);
                    case "louvain" -> buildLouvain(runId, graphName);
                    case "leiden"  -> buildLeiden(runId, graphName);
                    case "lpa"     -> buildLpa(runId, graphName);
                    case "chains"  -> buildChains(runId, graphName);
                    case "kcore"   -> buildKCore(runId, graphName, kValue);
                    default -> { continue; }
                }

                // Ensure full coverage so error is meaningful across all strategies
                addSingletonsForUncovered(runId, cand);

                long sN = count(
                        "MATCH (s:SuperNode {runId:$r, candidate:$c}) RETURN count(s) AS c",
                        Map.of("r",runId,"c",cand)
                );

                long sE = count(
                        "MATCH ()-[e:SUPER_EDGE {runId:$r, candidate:$c}]->() RETURN count(e) AS c",
                        Map.of("r",runId,"c",cand)
                );

                // ---- Fixed MDL accounting ----
                // coveredEdges: edges whose endpoints are both mapped to *some* supernode
                long coveredEdges = count(
                        "MATCH (a)-[rel]->(b) " +
                                "MATCH (a)-[:IN_SUPER {runId:$r, candidate:$c}]->(sa) " +
                                "MATCH (b)-[:IN_SUPER {runId:$r, candidate:$c}]->(sb) " +
                                "RETURN count(rel) AS c",
                        Map.of("r",runId,"c",cand)
                );

                // internalEdges: covered edges that become internal to one supernode (sa = sb)
                long internalEdges = count(
                        "MATCH (a)-[rel]->(b) " +
                                "MATCH (a)-[:IN_SUPER {runId:$r, candidate:$c}]->(sa) " +
                                "MATCH (b)-[:IN_SUPER {runId:$r, candidate:$c}]->(sb) " +
                                "WHERE sa = sb " +
                                "RETURN count(rel) AS c",
                        Map.of("r",runId,"c",cand)
                );

                // uncovered edges = error
                long error = Math.max(0, originalEdges - coveredEdges);

                // MDL = model cost + inter-summary cost + cost to encode internal structure + penalty for uncovered edges
                double score = (1.0 * sN) + (1.0 * sE) + (1.0 * internalEdges) + (2.0 * error);

                scoreByCand.put(cand, score);
                sizeByCand.put(cand, new long[]{sN, sE});

            } catch (Exception ex) {
                log.error("Erreur candidat " + cand + " : " + ex.getMessage(), ex);
                scoreByCand.put(cand, Double.POSITIVE_INFINITY);
                sizeByCand.put(cand, new long[]{0,0});
            }
        }

        // Best MDL = minimum
        String best = scoreByCand.entrySet().stream()
                .min(Comparator.comparingDouble(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .orElse(null);

        if (write && best != null) cleanupKeepBest(runId, best);

        return candidates.stream().map(c -> {
            long[] se = sizeByCand.getOrDefault(c, new long[]{0,0});
            double ratio = (originalNodes + originalEdges) == 0 ? 1.0 :
                    ((double)(se[0] + se[1])) / (double)(originalNodes + originalEdges);
            return new ResultRow(c, scoreByCand.getOrDefault(c, Double.POSITIVE_INFINITY), se[0], se[1], ratio);
        });
    }

    // -------- Builders (Community Detection & Structures) --------

    private void buildLeiden(String runId, String graphName) {
        String p = "c_le_" + shortId(runId);
        db.executeTransactionally("CALL gds.leiden.write($g, {writeProperty: $p})", Map.of("g", graphName, "p", p));
        buildSuperNodesFromProperty(runId, "leiden", p);
        createSuperEdges(runId, "leiden");
        removePropEverywhere(p);
    }

    private void buildLpa(String runId, String graphName) {
        String p = "c_lp_" + shortId(runId);
        db.executeTransactionally("CALL gds.labelPropagation.write($g, {writeProperty: $p})", Map.of("g", graphName, "p", p));
        buildSuperNodesFromProperty(runId, "lpa", p);
        createSuperEdges(runId, "lpa");
        removePropEverywhere(p);
    }

    private void buildWcc(String runId, String graphName) {
        String p = "c_wc_" + shortId(runId);
        db.executeTransactionally("CALL gds.wcc.write($g, {writeProperty:$p})", Map.of("g",graphName,"p",p));
        buildSuperNodesFromProperty(runId, "wcc", p);
        createSuperEdges(runId, "wcc");
        removePropEverywhere(p);
    }

    private void buildLouvain(String runId, String graphName) {
        String p = "c_lv_" + shortId(runId);
        db.executeTransactionally("CALL gds.louvain.write($g, {writeProperty:$p})", Map.of("g",graphName,"p",p));
        buildSuperNodesFromProperty(runId, "louvain", p);
        createSuperEdges(runId, "louvain");
        removePropEverywhere(p);
    }

    private void buildKCore(String runId, String graphName, int kIgnored) {
        String p = "c_kc_" + shortId(runId);
        db.executeTransactionally(
                "CALL gds.kcore.write($g, {writeProperty:$p})",
                Map.of("g", graphName, "p", p)
        );
        buildSuperNodesFromProperty(runId, "kcore", p);
        createSuperEdges(runId, "kcore");
        removePropEverywhere(p);
    }


    /** private void buildCliques(String runId, String graphName) {
        db.executeTransactionally("""
            CALL gds.alpha.maxcliques.stream($g) YIELD nodeIds
            WITH nodeIds, randomUUID() as gid
            WHERE size(nodeIds) > 2
            CREATE (s:SuperNode {runId:$r, candidate:'cliques', groupId:toString(gid), size:size(nodeIds)})
            WITH s, nodeIds
            UNWIND nodeIds AS nid
            MATCH (n) WHERE id(n) = nid
            MERGE (n)-[:IN_SUPER {runId:$r, candidate:'cliques'}]->(s)
            """, Map.of("r", runId, "g", graphName));
        createSuperEdges(runId, "cliques");
    }**/

    private void buildStars(String runId, long thr) {
        db.executeTransactionally("""
            MATCH (h)
            WITH h, COUNT {(h)--()} AS d
            WHERE d > $t
            CREATE (s:SuperNode {runId:$r, candidate:'stars', groupId:elementId(h), size:d+1})
            MERGE (h)-[:IN_SUPER {runId:$r, candidate:'stars'}]->(s)
            WITH h, s
            MATCH (h)--(n)
            MERGE (n)-[:IN_SUPER {runId:$r, candidate:'stars'}]->(s)
            """, Map.of("r", runId, "t", thr));
        createSuperEdges(runId, "stars");
    }

    /**
     * Chains: compute degree into a temporary property, then WCC only on degree==2 nodes.
     */
    /** private void buildChains(String runId, String graphName) {
        String degProp = "tmp_deg_" + shortId(runId);
        String compProp = "c_ch_" + shortId(runId);

        // write degree as a real DB property
        db.executeTransactionally(
                "CALL gds.degree.write($g, {writeProperty:$p})",
                Map.of("g", graphName, "p", degProp)
        );

        // nodeFilter uses DB property written above (property key can't be parameterized inside the string)
        String wccQuery =
                "CALL gds.wcc.write($g, {writeProperty:$p, nodeFilter:'n.`" + degProp + "` = 2'})";
        db.executeTransactionally(wccQuery, Map.of("g", graphName, "p", compProp));

        buildSuperNodesFromProperty(runId, "chains", compProp);
        createSuperEdges(runId, "chains");

        removePropEverywhere(compProp);
        removePropEverywhere(degProp);
    } **/

    private void buildChains(String runId, String graphName) {
        String degProp  = "tmp_deg_" + shortId(runId);
        String compProp = "tmp_wcc_" + shortId(runId);

        // 1) Degree
        db.executeTransactionally(
                "CALL gds.degree.write($g, {writeProperty:$p})",
                Map.of("g", graphName, "p", degProp)
        );

        // 2) WCC on all nodes (no nodeFilter supported in your GDS)
        db.executeTransactionally(
                "CALL gds.wcc.write($g, {writeProperty:$p})",
                Map.of("g", graphName, "p", compProp)
        );

        // 3) Build chain supernodes from nodes with degree==2, grouped by WCC id
        String q =
                "MATCH (n) " +
                        "WHERE n.`" + degProp + "` = 2 AND n.`" + compProp + "` IS NOT NULL " +
                        "WITH n.`" + compProp + "` AS gid, collect(n) AS nodes " +
                        "WHERE size(nodes) > 1 " +
                        "CREATE (s:SuperNode {runId:$r, candidate:'chains', groupId:toString(gid), size:size(nodes)}) " +
                        "WITH s, nodes UNWIND nodes AS n " +
                        "MERGE (n)-[:IN_SUPER {runId:$r, candidate:'chains'}]->(s)";
        db.executeTransactionally(q, Map.of("r", runId));

        addSingletonsForUncovered(runId, "chains");
        createSuperEdges(runId, "chains");

        removePropEverywhere(degProp);
        removePropEverywhere(compProp);
    }


    // -------- Helpers --------

    private void buildSuperNodesFromProperty(String runId, String cand, String prop) {
        String q =
                "MATCH (n) WHERE n.`" + prop + "` IS NOT NULL " +
                        "WITH n.`" + prop + "` AS gid, collect(n) AS nodes " +
                        "CREATE (s:SuperNode {runId:$r, candidate:$c, groupId:toString(gid), size:size(nodes)}) " +
                        "WITH s, nodes UNWIND nodes AS n " +
                        "MERGE (n)-[:IN_SUPER {runId:$r, candidate:$c}]->(s)";
        db.executeTransactionally(q, Map.of("r", runId, "c", cand));
    }

    private void addSingletonsForUncovered(String runId, String cand) {
        db.executeTransactionally(
                "MATCH (n) " +
                        "WHERE NOT (n)-[:IN_SUPER {runId:$r, candidate:$c}]->() " +
                        "CREATE (s:SuperNode {runId:$r, candidate:$c, groupId:elementId(n), size:1}) " +
                        "MERGE (n)-[:IN_SUPER {runId:$r, candidate:$c}]->(s)",
                Map.of("r", runId, "c", cand)
        );
    }

    private void createSuperEdges(String runId, String cand) {
        db.executeTransactionally(
                "MATCH (a)-[rel]->(b) " +
                        "MATCH (a)-[:IN_SUPER {runId:$rid, candidate:$can}]->(sa) " +
                        "MATCH (b)-[:IN_SUPER {runId:$rid, candidate:$can}]->(sb) " +
                        "WHERE sa <> sb " +
                        "MERGE (sa)-[e:SUPER_EDGE {runId:$rid, candidate:$can}]->(sb) " +
                        "ON CREATE SET e.weight=1 " +
                        "ON MATCH SET e.weight=e.weight+1",
                Map.of("rid", runId, "can", cand)
        );
    }

    private void removePropEverywhere(String prop) {
        String q = "MATCH (n) WHERE n.`" + prop + "` IS NOT NULL REMOVE n.`" + prop + "`";
        db.executeTransactionally(q);
    }

    private void cleanupCandidate(String runId, String cand) {
        db.executeTransactionally(
                "MATCH (s:SuperNode {runId:$r, candidate:$c}) DETACH DELETE s",
                Map.of("r", runId, "c", cand)
        );
    }

    private void cleanupKeepBest(String runId, String best) {
        db.executeTransactionally(
                "MATCH (s:SuperNode {runId:$r}) WHERE s.candidate <> $b DETACH DELETE s",
                Map.of("r", runId, "b", best)
        );
    }

    private long count(String q, Map<String,Object> p) {
        return db.executeTransactionally(q, p, res -> {
            if (!res.hasNext()) return 0L;
            Object v = res.next().get("c");
            if (v == null) return 0L;
            if (v instanceof Number n) return n.longValue();
            return Long.parseLong(v.toString());
        });
    }

    private String shortId(String id) {
        return id.replace("-", "").substring(0, 6);
    }
}
