package partitionnumbers;

import clusteringbasedfragmentation.Cluster;
import clusteringbasedfragmentation.Clustering;
import clusteringbasedfragmentation.ClusteringAffinityFunction;
import clusteringbasedfragmentation.SimilarityException;
import clusteringbasedfragmentation.similarityfunctions.SimClusteringTableSimilarity;
import neo4j.PathLengthCSV;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import rewriting.RelaxationAttributeSelectionFinder;
import utils.IgniteUtils;
import utils.SQLQueryUtils;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * This class provides the query metrics for the partition number approach.
 */
public class QueryMetrics {


    /**
     * Query timeout in seconds.
     */
    private static final int QUERY_TIMEOUT = 600;


    /**
     * Print the query metrics to System.out
     *
     * @param affinityFunction Clustering affinity function
     * @throws ClassNotFoundException Thrown if Ignite's JDBC driver can not be found
     * @throws SQLException           Error when parsing or executing the query
     */
    public static void printQueryMetrics(ClusteringAffinityFunction affinityFunction)
            throws ClassNotFoundException, SQLException {

        // Register driver
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Returns connection to the cluster (Port 10800 default for JDBC client) TODO
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://141.5.107.8, 141.5.107.75, 141.5.107.76;" +
                "collocated=true;");
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(QUERY_TIMEOUT);

        // Get some diseases d1, d2, d3 from clustering where d1 and d2 are in the same cluster but d3 is in another one
        String d1, d2, d3;
        int clusternumber = 0;
        Clustering clustering = affinityFunction.getClustering();
        while (clustering.getCluster(clusternumber).getAdom().size() < 1)
            clusternumber++;
        Cluster<String> cluster = clustering.getCluster(clusternumber);
        Cluster<String> cluster2 = clustering.getCluster(clusternumber + 1);
        d1 = SQLQueryUtils.escape(cluster.getHead());
        d2 = SQLQueryUtils.escape(cluster.getAdom().iterator().next());
        d3 = SQLQueryUtils.escape(cluster2.getHead());

        // Sample queries
        ArrayList<String> queries = new ArrayList<>(Arrays.asList(

                "SELECT p.name, p.age, p.address FROM ILL i, INFO p WHERE i.id = p.id AND i.disease='" + d1 + "'"
                , "SELECT p.name, p.age, p.address FROM ILL i1, ILL i2, INFO p " +
                        "WHERE i1.id = p.id AND i2.id = p.id " +
                        "AND i1.disease='" + d1 + "' AND i2.disease='" + d3 + "'"
                , "SELECT t.prescription FROM ILL i,TREAT t WHERE t.id = i.id AND i.disease = '" + d1 + "'"
                , "SELECT p.name, p.age, t.prescription FROM ILL i,TREAT t, INFO p " +
                        "WHERE t.id = i.id AND i.id = p.id AND i.disease = '" + d1 + "'"
                , "SELECT t.prescription FROM ILL i, ILL i2, TREAT t " +
                        "WHERE i.id = i2.id AND i.id = t.id AND i.disease = '" + d1 + "' " +
                        "AND i2.disease = '" + d2 + "'"
        ));

        // Execute all queries once and print the result
        for (String query : queries) {
            System.out.println("Query: " + query);
            ResultSet res = stmt.executeQuery(query);
            SQLQueryUtils.printResultSet(res);
        }


        // Execute each query 10 times and calculate the average execution time
        long[] times;
        long before, after;
        long[] avgTimes = new long[queries.size()];
        System.out.println("Executing " + queries.size() + " queries ...");
        for (int i = 0; i < queries.size(); i++) {

            // Init and output
            times = new long[3];
            String query = queries.get(i);
            System.out.println("Executing query: " + query);

            // Execute and measure time
            for (int j = 0; j < times.length; j++) {
                before = System.nanoTime();
                try {
                    stmt.executeQuery(query);
                } catch (SQLTimeoutException e) {
                    // Timeout handling
                    for (int k = j; k < times.length; k++) {
                        times[k] = QUERY_TIMEOUT * (long) 10e9;     // set each value to timeout
                    }
                    break;
                }
                after = System.nanoTime();
                times[j] = after - before;

                if (times[j] / 1000000000.0 >= QUERY_TIMEOUT) {
                    // Timeout handling
                    for (int k = j; k < times.length; k++) {
                        times[k] = QUERY_TIMEOUT * (long) 10e9;     // set each value to timeout
                    }
                    break;
                }
            }

            // Calculate average
            long sum = 0;
            for (long t : times)
                sum += t;
            avgTimes[i] = sum / (long) times.length;
        }


        // Print results
        System.out.println("\n\n##### Average Query Execution Times ######");
        for (int i = 0; i < queries.size(); i++) {
            System.out.println("Query: " + queries.get(i) + ", Avg. Time=" + avgTimes[i] / 1000000000.0 + "s");
        }

        stmt.close();
        conn.close();
    }


    /**
     * Print the query metrics using the Ignite API instead of the JDBC connection
     *
     * @param affinityFunction Clustering affinity function
     */
    public static void printQueryMetricsAPI(ClusteringAffinityFunction affinityFunction) {

        // Get some diseases d1, d2, d3 from clustering where d1 and d2 are in the same cluster but d3 is in another one
        String d1, d2, d3;
        int clusternumber = 0;
        Clustering clustering = affinityFunction.getClustering();
        while (clustering.getCluster(clusternumber).getAdom().size() < 1)
            clusternumber++;
        Cluster<String> cluster = clustering.getCluster(clusternumber);
        Cluster<String> cluster2 = clustering.getCluster(clusternumber + 1);
        d1 = SQLQueryUtils.escape(cluster.getHead());
        d2 = SQLQueryUtils.escape(cluster.getAdom().iterator().next());
        d3 = SQLQueryUtils.escape(cluster2.getHead());


        // Sample queries
        ArrayList<String> queries = new ArrayList<>(Arrays.asList(

                "SELECT p.name, p.age, p.address FROM ILL i, INFO p WHERE i.id = p.id AND i.disease='" + d1 + "'"
                , "SELECT p.name, p.age, p.address FROM ILL i1, ILL i2, INFO p " +
                        "WHERE i1.id = p.id AND i2.id = p.id " +
                        "AND i1.disease='" + d1 + "' AND i2.disease='" + d3 + "'"
                , "SELECT t.prescription FROM ILL i,TREAT t WHERE t.id = i.id AND i.disease = '" + d1 + "'"
                , "SELECT p.name, p.age, t.prescription FROM ILL i,TREAT t, INFO p " +
                        "WHERE t.id = i.id AND i.id = p.id AND i.disease = '" + d1 + "'"
                , "SELECT t.prescription FROM ILL i, ILL i2, TREAT t " +
                        "WHERE i.id = i2.id AND i.id = t.id AND i.disease = '" + d1 + "' " +
                        "AND i2.disease = '" + d2 + "'"
        ));

        // Connect client TODO
        HashSet<String> addresses = new HashSet<>();
        addresses.add("141.5.107.8:47500..47509");
        addresses.add("141.5.107.75:47500..47509");
        addresses.add("141.5.107.76:47500..47509");
        IgniteConfiguration clientConfig = IgniteUtils.createIgniteConfig(addresses, true);

        try (Ignite client = Ignition.start(clientConfig)) {

            IgniteCache illCache = client.cache("SQL_PUBLIC_ILL");

            for (String q : queries) {

                // Find relax. attr. selection conditions --> identify cluster & partition
                System.out.println("##########\nTry to execute query: " + q);
                try {

                    // Parse query to obtain selection conditions on relax. attribute
                    Select stmt = (Select) CCJSqlParserUtil.parse(q);
                    PlainSelect body = (PlainSelect) stmt.getSelectBody();
                    RelaxationAttributeSelectionFinder finder = new RelaxationAttributeSelectionFinder();

                    // Found some selections?
                    int[] partitions = null;
                    HashSet<Integer> partitionSet;
                    if (finder.findRelaxationAttributeSelections(body.getWhere())) {

                        // Get all disease terms and corresponding partitions to the found selections
                        ArrayList<EqualsTo> selections = finder.getRelaxationAttributeSelections();
                        System.out.println("Selection Conditions: " + Arrays.toString(selections.toArray()));   // DEBUG
                        partitionSet = SQLQueryUtils.getPartitionsForSelections(selections, affinityFunction);
                        partitions = new int[partitionSet.size()];
                        int i = 0;
                        for (Integer p : partitionSet) {
                            partitions[i] = p;
                            i++;
                        }
                        Arrays.sort(partitions);
                    }

                    // Query & Result
                    SqlFieldsQuery query = new SqlFieldsQuery(q);
                    if (partitions != null)
                        query.setPartitions(partitions);
                    query.setCollocated(true);

                    // Execute 10 times
                    long[] times = new long[3];
                    long before, after;
                    for (int i = 0; i < times.length; i++) {
                        before = System.nanoTime();
                        illCache.query(query).getAll();
                        times[i] = System.nanoTime() - before;
                    }

                    // Metrics
                    long avg = 0;
                    for (long t : times)
                        avg += t;
                    avg = avg / (long) times.length;
                    System.out.println("Query: " + q + ", Avg. Time: " + avg / 1000000000.0 + "s");

                } catch (JSQLParserException e) {
                    e.printStackTrace();
                }
            }
        } catch (IgniteException e) {
            e.printStackTrace();
        }
    }


    /**
     * Print the query metrics for the flexible query answering (with API and JDBC)
     *
     * @param affinityFunction Clustering affinity function
     * @throws JSQLParserException    Error upon query parsing/generalization
     * @throws ClassNotFoundException Thrown when the JDBC driver of Ignite was not found
     * @throws SQLException           Error when parsing or executing the query
     */
    public static void printFlexibleQueryMetrics(ClusteringAffinityFunction affinityFunction) throws JSQLParserException, ClassNotFoundException, SQLException {

        // Get some diseases d1, d2 from clustering that are in the same cluster
        String d1, d2;
        int clusternumber = 0;
        Clustering clustering = affinityFunction.getClustering();
        while (clustering.getCluster(clusternumber).getAdom().size() < 1)
            clusternumber++;
        Cluster<String> cluster = clustering.getCluster(clusternumber);
        d1 = SQLQueryUtils.escape(cluster.getHead());
        d2 = SQLQueryUtils.escape(cluster.getAdom().iterator().next());

        List<String> queries = Arrays.asList(
                "SELECT p.* FROM INFO p, ILL i WHERE p.id = i.id AND i.disease = '" + d1 + "'"
                , "SELECT p.* FROM INFO p, ILL i WHERE p.id = i.id AND i.disease = '" + d1 + "' AND p.age < 50"
                , "SELECT p.* FROM INFO p, ILL i1, ILL i2 WHERE p.id = i1.id AND p.id = i2.id AND " +
                        "i1.disease = '" + d1 + "' AND i2.disease = '" + d2 + "'"
        );

        IgniteConfiguration config = IgniteUtils.createIgniteConfig(Arrays.asList("141.5.107.8:47500..47509",
                "141.5.107.75:47500..47509", "141.5.107.76:47500..47509"), true);
        try (Ignite client = Ignition.start(config)) {

            IgniteCache cache = client.cache("SQL_PUBLIC_ILL");

            for (String q : queries) {

                System.out.println("######\nQuery: " + q);  // no need for rewriting

                // Find the relevant fragments/partitions
                Select select = (Select) CCJSqlParserUtil.parse(q);
                PlainSelect body = (PlainSelect) select.getSelectBody();
                RelaxationAttributeSelectionFinder finder = new RelaxationAttributeSelectionFinder();
                int[] partitions = null;
                HashSet<Integer> partitionSet;
                if (finder.findRelaxationAttributeSelections(body.getWhere())) {

                    // Get all disease terms and corresponding partitions to the found selections
                    ArrayList<EqualsTo> selections = finder.getRelaxationAttributeSelections();
                    partitionSet = SQLQueryUtils.getPartitionsForSelections(selections, affinityFunction);
                    int i = 0;
                    for (Integer p : partitionSet) {
                        partitions[i] = p;
                        i++;
                    }
                    Arrays.sort(partitions);
                }
                String generalized = FlexibleQueryAnswering.generalize(q, affinityFunction);
                System.out.println("Generalized query: " + generalized);
                SqlFieldsQuery query = new SqlFieldsQuery(generalized);
                query.setCollocated(true).setPartitions(partitions);

                // Execute query 10 times
                long[] times = new long[3];
                long before, after;
                for (int i = 0; i < times.length; i++) {
                    before = System.nanoTime();
                    cache.query(query).getAll();
                    times[i] = System.nanoTime() - before;
                }

                long avg = 0;
                for (long t : times)
                    avg += t;
                avg = avg / (long) times.length;
                System.out.println("Avg. Time: " + avg / 1000000000.0 + "s");

            }
        }

        System.out.println("############################################################");

        // Register driver
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Returns connection to the cluster (Port 10800 default for JDBC client) TODO
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://141.5.107.8, 141.5.107.75, 141.5.107.76;" +
                "collocated=true;");
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(QUERY_TIMEOUT);

        // Same for every query
        for (String q : queries) {

            System.out.println("######\nQuery: " + q);
            String generalized = FlexibleQueryAnswering.generalize(q, affinityFunction);
            System.out.println("Generalized Query: " + generalized);

            // Execute query 10 times
            long[] times = new long[3];
            long before;
            for (int i = 0; i < times.length; i++) {
                before = System.nanoTime();
                stmt.executeQuery(generalized);
                times[i] = System.nanoTime() - before;
            }

            long avg = 0;
            for (long t : times)
                avg += t;
            avg = avg / (long) times.length;
            System.out.println("Avg. Time: " + avg / 1000000000.0 + "s");

        }

        conn.close();
    }


    /**
     * Test unit.
     *
     * @param args Not used.
     */
    public static void main(String[] args) throws SQLException, ClassNotFoundException, JSQLParserException,
            SimilarityException, IOException {

        // TODO must coincide with actual used term set
        String pathlengthCSV = "csv/pathlengths2500.csv";
        String clustering = "clustering/clustering2500";

        SimClusteringTableSimilarity similarity = new SimClusteringTableSimilarity("SIM",
                PathLengthCSV.readTermSet(pathlengthCSV), false, "CLUSTERING",
                null, true);
        ClusteringAffinityFunction aff = new ClusteringAffinityFunction(similarity, clustering);
        QueryMetrics.printQueryMetrics(aff);
        QueryMetrics.printQueryMetricsAPI(aff);
//        printFlexibleQueryMetrics(aff);
    }
}
