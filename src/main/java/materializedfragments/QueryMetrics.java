package materializedfragments;

import clusteringbasedfragmentation.Cluster;
import clusteringbasedfragmentation.ClusteringAffinityFunction;
import clusteringbasedfragmentation.SimilarityException;
import clusteringbasedfragmentation.similarityfunctions.SimClusteringTableSimilarity;
import neo4j.PathLengthCSV;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import rewriting.QueryRewriter;
import rewriting.RelaxationAttributeSelectionFinder;
import utils.IgniteUtils;
import utils.SQLQueryUtils;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Query metrics for materialized fragment approach.
 */
public class QueryMetrics {


    /**
     * Query timeout in seconds.
     */
    private static final int QUERY_TIMEOUT = 600;


    /**
     * Print Query metrics for some sample queries.
     * @param affinityFunction Affinity Function
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static void printQueryMetrics(ClusteringAffinityFunction affinityFunction) throws ClassNotFoundException, SQLException {

        // Register driver
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Returns connection to the cluster (Port 10800 default for JDBC client)
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://141.5.107.8, 141.5.107.75, " +
                "141.5.107.76;collocated=true");
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(QUERY_TIMEOUT);

        // Get some diseases d1, d2, d3 from clustering where d1 and d2 are in the same cluster but d3 is in another one
        String d1, d2, d3;
        int clusternumber = 0;
        while (affinityFunction.getClusters().get(clusternumber).getAdom().size() < 1)
            clusternumber++;
        Cluster<String> cluster = affinityFunction.getClusters().get(clusternumber);
        Cluster<String> cluster2 = affinityFunction.getClusters().get(clusternumber + 1);
        d1 = cluster.getHead();
        d2 = cluster.getAdom().iterator().next();
        d3 = cluster2.getHead();


        // Sample queries
        ArrayList<String> queries = new ArrayList<>(Arrays.asList(

                "SELECT p.name, p.age, p.address FROM ILL i, INFO p WHERE i.id = p.id AND i.disease='" + d1 + "'"
                ,"SELECT p.name, p.age, p.address FROM ILL i1, ILL i2, INFO p "+
                        "WHERE i1.id = p.id AND i2.id = p.id " +
                        "AND i1.disease='" + d1 + "' AND i2.disease='" + d3 + "'"
                ,"SELECT t.prescription FROM ILL i,TREAT t WHERE t.id = i.id AND i.disease = '" + d1 + "'"
                ,"SELECT p.name, p.age, t.prescription FROM ILL i,TREAT t, INFO p " +
                        "WHERE t.id = i.id AND i.id = p.id AND i.disease = '" + d1 + "'"
                ,"SELECT t.prescription FROM ILL i, ILL i2, TREAT t " +
                        "WHERE i.id = i2.id AND i.id = t.id AND i.disease = '"+ d1 +"' " +
                        "AND i2.disease = '" + d2 + "'"
        ));
        QueryRewriter rewriter = new QueryRewriter(affinityFunction);
        List<String> rewrittenQueries = new ArrayList<>();

        // Rewrite
        for (String sql : queries) {
            try {
                rewrittenQueries.add(rewriter.rewrite(sql));
            } catch (JSQLParserException e) {
                System.err.println("Parsing Error for query: " + sql);
                e.printStackTrace();
            } catch (SimilarityException e) {
                System.err.println("Similarity Error for query: " + sql);
                e.printStackTrace();
            }
        }


        // Execute each query once, print result
        for (String query : rewrittenQueries) {
            System.out.println("Query: " + query);
            ResultSet res = stmt.executeQuery(query);
            SQLQueryUtils.printResultSet(res);
        }


        // Execute each query 10 times and calculate the average execution time
        long[] times, apiTimes;
        long before, after;
        long[] avgTimes = new long[rewrittenQueries.size()];
        System.out.println("Executing " + rewrittenQueries.size() + " queries ...");
        for (int i = 0; i < rewrittenQueries.size(); i++) {

            // Init and output
            times = new long[3];
            System.out.println("Executing query: " + queries.get(i));
            String query = rewrittenQueries.get(i);
            System.out.println("Rewritten: " + query);

            // Execute and measure time
            for (int j = 0; j < times.length; j++) {
                before = System.nanoTime();
                try {
                    ResultSet res = stmt.executeQuery(query);
                    SQLQueryUtils.fetchAll(res);
                }catch (SQLTimeoutException e) {
                    // Timeout handling
                    for (int k = j; k < times.length; k++) {
                        times[k] = QUERY_TIMEOUT * (long) 10e8;     // set each value to timeout
                    }
                    break;
                }
                after = System.nanoTime();
                times[j] = after - before;


                if (times[j] / 1000000000.0 >= QUERY_TIMEOUT) {
                    // Timeout handling
                    for (int k = j; k < times.length; k++) {
                        times[k] = QUERY_TIMEOUT * (long) 10e8;     // set each value to timeout
                    }
                    break;
                }
            }

            // Avg
            avgTimes[i] = SQLQueryUtils.avg(times);
            System.out.println("Avg. time: " + avgTimes[i] / 1000000000.0 + "s");
        }


        // Execute via SqlFieldsQuery
        IgniteConfiguration config = IgniteUtils.createIgniteConfig(Arrays.asList("141.5.107.8", "141.5.107.75",
                "141.5.107.76"), true);     // TOD
        apiTimes = new long[rewrittenQueries.size()];
        try (Ignite client = Ignition.start(config)) {

            // Cache
            IgniteCache cache = client.cache("SQL_PUBLIC_ILL_0");

            // Execute each query 10 times
            System.out.println("Executing " + rewrittenQueries.size() + " queries (API) ...");
            for (int i = 0; i < rewrittenQueries.size(); i++) {
                times = new long[3];
                System.out.println("Executing query: " + queries.get(i));
                String query = rewrittenQueries.get(i);
                System.out.println("Rewritten: " + query);
                SqlFieldsQuery fieldsQuery = new SqlFieldsQuery(query);

                // Find partitions (if possible)
                Select select = (Select) CCJSqlParserUtil.parse(queries.get(i));
                PlainSelect body = (PlainSelect) select.getSelectBody();
                RelaxationAttributeSelectionFinder finder  = new RelaxationAttributeSelectionFinder();
                if (finder.findRelaxationAttributeSelections(body.getWhere())) {
                    ArrayList<EqualsTo> selections = finder.getRelaxationAttributeSelections();
                    HashSet<Integer> partitionSet = new HashSet<>();
                    for (int j = 0; j < selections.size(); j++) {
                        EqualsTo eq = selections.get(j);
                        String disease = ((StringValue) eq.getRightExpression()).getValue();
                        partitionSet.add(affinityFunction.identifyCluster(disease));
                    }
                    int[] partitions = new int[partitionSet.size()];
                    int j = 0;
                    for (Integer p : partitionSet) {
                        partitions[j] = p;
                        j++;
                    }
                    fieldsQuery.setPartitions(partitions);
//                    fieldsQuery.setDistributedJoins(true);    //Using both partitions and distributed JOINs is not supported for the same query
                }

                // Execute and measure time
                for (int j = 0; j < times.length; j++) {
                    before = System.nanoTime();
                    cache.query(fieldsQuery).getAll();
                    times[j] = System.nanoTime() - before;
                }

                // Calculate average
                apiTimes[i] = SQLQueryUtils.avg(times);
                System.out.println("Avg. time: " + apiTimes[i] / 1000000000.0 + "s");
            }


        } catch (JSQLParserException e) {
            e.printStackTrace();
        } catch (SimilarityException e) {
            e.printStackTrace();
        }


        //Print results again
        System.out.println("\n\n##### Average Query Execution Times ######");
        for (int i = 0; i < queries.size(); i++) {
            System.out.println("Query: " + queries.get(i) + ", Avg. Time=" + avgTimes[i] / 1000000000.0 + "s, " +
                    "Avg. Time (API)=" + apiTimes[i] / 1000000000.0 + "s");

        }

        stmt.close();
        conn.close();
    }


    /**
     * Print Query metrics for some sample flexibly answered queries.
     * @param affinityFunction Affinity Function
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws JSQLParserException
     */
    public static void printFlexibleAnsweringMetrics(ClusteringAffinityFunction affinityFunction)
            throws SQLException, ClassNotFoundException, JSQLParserException, SimilarityException {

        // Queries
        List<String> queries = Arrays.asList(
                "SELECT p.Name, p.Age FROM ILL i, INFO p WHERE i.id = p.id AND i.disease = 'Liver Failure'"
                ,"SELECT p.Name, p.Age FROM ILL i, INFO p WHERE i.id = p.id AND i.disease = 'Liver Failure' " +
                        "AND p.AGE < 50"
                ,"SELECT p1.* FROM  ILL i1, ILL i2, INFO p1, INFO p2 WHERE i1.id = p1.id " +
                        "AND i2.id = p2.id AND i1.id = i2.id AND i1.disease='Liver Failure' AND i2.disease='Hemoptysis'"
        );

        // Register driver & get connection
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://141.5.107.8, 141.5.107.75, " +
                "141.5.107.76;collocated=true");        // TODO
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(QUERY_TIMEOUT);

        // Generalize & execute each query ..
        QueryRewriter rewriter = new QueryRewriter(affinityFunction);
        for (String q : queries) {

            // Generalization
            System.out.println("#######\nQuery: " + q);
            String generalized = rewriter.rewrite(q);
            generalized = FlexibleQueryAnswering.generalize(q, affinityFunction);
            System.out.println("Rewritten & Generalized Query: " + generalized);

            // Execute 10 times
            long[] times = new long[10];
            long before;
            for (int i = 0; i < 10; i++) {
                before = System.nanoTime();
                ResultSet res = stmt.executeQuery(generalized);
                List<List<?>> allTuples = SQLQueryUtils.fetchAll(res);
                times[i] = System.nanoTime() - before;
            }

            // Avg
            long avg = SQLQueryUtils.avg(times);
            System.out.println("Avg. Time: " + avg / 1000000000.0 + "s");
        }

    }



    /**
     * Test unit.
     * @param args Not used
     */
    public static void main(String[] args) throws SQLException, ClassNotFoundException, JSQLParserException,
            SimilarityException, IOException {

        // TODO must coincide with actual used term set
        String pathlengthCSV = "csv" + File.separator + "pathlengths500.csv";
        String clustering = "clustering" + File.separator + "clustering500";

        SimClusteringTableSimilarity similarity = new SimClusteringTableSimilarity("SIM",
                PathLengthCSV.readTermSet(pathlengthCSV), false, "CLUSTERING",
                null, true);
        ClusteringAffinityFunction aff = new ClusteringAffinityFunction(similarity, clustering);
//        printQueryMetrics(aff);
        printFlexibleAnsweringMetrics(aff);
    }
}
