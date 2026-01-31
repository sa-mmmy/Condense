package com.lyon1.condense;

import org.junit.jupiter.api.*;
import org.neo4j.driver.*;
import org.neo4j.harness.*;

import static org.junit.jupiter.api.Assertions.*;

public class CondenseProcTest {

    private static Neo4j embedded;
    private static Driver driver;

    @BeforeAll
    static void setup() {
        embedded = Neo4jBuilders.newInProcessBuilder()
                .withProcedure(CondenseProc.class)   // registers your procedure
                .build();

        driver = GraphDatabase.driver(embedded.boltURI(), Config.builder().withoutEncryption().build());
    }

    @AfterAll
    static void teardown() {
        if (driver != null) driver.close();
        if (embedded != null) embedded.close();
    }

    //@Test
    //void runsOnSmallGraph() {
    //    try (Session session = driver.session()) {
     //       // Create a small graph
       //     session.run("""
         //       CREATE (a:Node {id:1}), (b:Node {id:2}), (c:Node {id:3}), (d:Node {id:4})
           //     CREATE (a)-[:R]->(b)
             //   CREATE (a)-[:R]->(c)
              //  CREATE (b)-[:R]->(c)
            //    CREATE (c)-[:R]->(d)
             //   """).consume();

            // Call your procedure (use only 'stars' if you didn't install GDS)
           // var result = session.run("""
             //   CALL condense.run({candidates:['stars'], degreeThreshold:1, write:true})
               // YIELD candidate, mdlScore, superNodes, superEdges, compressionRatio
              //  RETURN candidate, mdlScore, superNodes, superEdges, compressionRatio
                //""");

          //  var rec = result.single();
          //  assertEquals("stars", rec.get("candidate").asString());
          //  assertTrue(rec.get("superNodes").asLong() >= 1);

            // Verify that supergraph nodes were written
            // var count = session.run("MATCH (s:SuperNode) RETURN count(s) AS c").single().get("c").asLong();
          //  assertTrue(count > 0);
        //}



   // }

    @Test
    void runsAndPrintsResults() {
        try (Session session = driver.session()) {

            // 1) Build a slightly bigger graph so "stars" has something to do
            session.run("""
            CREATE (c:Node {id:0})
            WITH c
            UNWIND range(1,12) AS i
            CREATE (n:Node {id:i})
            CREATE (c)-[:R]->(n)
            """).consume();

            // Add a few extra edges between leaves (to make it less trivial)
            session.run("""
            MATCH (a:Node {id:1}), (b:Node {id:2}), (d:Node {id:3})
            CREATE (a)-[:R]->(b)
            CREATE (b)-[:R]->(d)
            """).consume();

            // 2) Run your procedure
            var result = session.run("""
            CALL condense.run({
              candidates:['stars'],
              degreeThreshold:3,
              write:true
            })
            YIELD candidate, mdlScore, superNodes, superEdges, compressionRatio
            RETURN candidate, mdlScore, superNodes, superEdges, compressionRatio
            ORDER BY mdlScore ASC
            """);

            var rows = result.list();

            // 3) Print all rows (this is what you wanted)
            System.out.println("=== CONDENSE RESULTS ===");
            for (var r : rows) {
                System.out.println(
                        "candidate=" + r.get("candidate").asString() +
                                " mdlScore=" + r.get("mdlScore").asDouble() +
                                " superNodes=" + r.get("superNodes").asLong() +
                                " superEdges=" + r.get("superEdges").asLong() +
                                " compressionRatio=" + r.get("compressionRatio").asDouble()
                );
            }

            // 4) Winner = first row because we ORDER BY mdlScore
            var winner = rows.get(0).get("candidate").asString();
            System.out.println("WINNER = " + winner);

            // 5) Basic sanity checks
            assertFalse(rows.isEmpty());
            assertNotNull(winner);

            // optional: verify it wrote something
            long superCount = session.run("MATCH (s:SuperNode) RETURN count(s) AS c")
                    .single().get("c").asLong();
            System.out.println("SuperNodes written = " + superCount);
            assertTrue(superCount > 0);
        }
    }

}
