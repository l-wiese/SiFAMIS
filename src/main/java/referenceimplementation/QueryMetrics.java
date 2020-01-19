package referenceimplementation;

import clusteringbasedfragmentation.ClusteringAffinityFunction;
import clusteringbasedfragmentation.SimilarityException;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import utils.IgniteUtils;
import utils.SQLQueryUtils;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * This class provides the query metrics for the basic Ignite approach.
 */
public class QueryMetrics {

    /**
     * Query timeout in seconds.
     */
    private static final int QUERY_TIMEOUT = 600;


    /**
     * Print query metrics using a JDBC connection to the cluster
     *
     * @param address Address string for Ignite cluster (JDBC)
     * @throws ClassNotFoundException Thrown if Ignite's JDBC driver was not found
     * @throws SQLException           Error when parsing or executing the query
     */
    public static void printJDBCMetrics(String address) throws ClassNotFoundException, SQLException {
        // Register driver
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Return connection to the cluster (Port 10800 default for JDBC client, default schema is PUBLIC)
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + address + ";distributedJoins=true;");
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(QUERY_TIMEOUT);

        // Queries
        ArrayList<String> queries = new ArrayList<>(Arrays.asList(

                "SELECT p.name, p.age, p.address FROM ILL i, INFO p WHERE i.id = p.id AND i.disease='Liver Failure'"
                , "SELECT p.name, p.age, p.address FROM ILL i1, ILL i2, INFO p " +
                        "WHERE i1.id = p.id AND i2.id = p.id " +
                        "AND i1.disease='Liver Failure' AND i2.disease='Hemoptysis'"
                , "SELECT t.prescription FROM ILL i,TREAT t WHERE t.id = i.id AND i.disease = 'Liver Failure'"
                , "SELECT p.name, p.age, t.prescription FROM ILL i,TREAT t, INFO p " +
                        "WHERE t.id = i.id AND i.id = p.id AND i.disease = 'Liver Failure'"
                , "SELECT t.prescription FROM ILL i, ILL i2, TREAT t " +
                        "WHERE i.id = i2.id AND i.id = t.id AND i.disease = 'Liver Failure' " +
                        "AND i2.disease = 'Liver Diseases, Alcoholic'"
        ));


        // Execute each query 10 times and calculate the average execution time
        long[] times;
        long before;
        long[] avgTimes = new long[queries.size()];
        System.out.println("Executing " + queries.size() + " queries ...");
        for (String q : queries) {
            times = new long[10];
            System.out.println("Executing query: " + q);
            // Execute and measure time
            for (int i = 0; i < times.length; i++) {
                before = System.nanoTime();
                try {
                    ResultSet res = stmt.executeQuery(q);
                    SQLQueryUtils.fetchAll(res);
                } catch (SQLTimeoutException e) {
                    // Timeout handling
                    for (int j = i; j < times.length; j++) {
                        times[j] = QUERY_TIMEOUT * (long) 10e9;     // set each value to timeout
                    }
                    break;
                }
                times[i] = System.nanoTime() - before;

                if (times[i] / 1000000000.0 >= QUERY_TIMEOUT) {
                    // Timeout handling
                    for (int k = i; k < times.length; k++) {
                        times[k] = QUERY_TIMEOUT * (long) 10e9;     // set each value to timeout
                    }
                    break;
                }
            }

            // Avg. Time
            avgTimes[queries.indexOf(q)] = SQLQueryUtils.avg(times);
            System.out.println("Avg. Time=" + avgTimes[queries.indexOf(q)] / 1000000000.0 + "s");
        }


        // Print results
        System.out.println("Average Query Execution Times: ");
        for (String q : queries) {
            System.out.println("Query: " + q + ", Avg. Time=" + avgTimes[queries.indexOf(q)] / 1000000000.0 + "s");
        }
    }


    /**
     * Print query metrics using the Ignite API
     *
     * @param addresses Addresses of the Ignite servers
     */
    public static void printFieldsQueryMetrics(Collection<String> addresses) {

        // Queries
        List<String> queries = Arrays.asList(
                "SELECT avg(p.age) FROM INFO p",
                "SELECT DISTINCT p.Name, p.age, p.address FROM ILL i, INFO p WHERE i.id = p.id AND i.disease='Liver Failure'",
                "SELECT DISTINCT p.name, p.age, p.address FROM ILL i1, ILL i2, INFO p WHERE i1.id = p.id " +
                        "AND i2.id = p.id AND i1.disease='Liver Failure' AND i2.disease='Hemoptysis'",
                "SELECT i.Disease, p.* FROM ILL i, INFO p WHERE i.id = p.id",
                "SELECT p.Name, count(i.disease) FROM ILL i, INFO p WHERE i.id = p.id " +
                        "GROUP BY p.Name ORDER BY count(i.disease)",
                "SELECT p.name, i1.disease, i2.disease FROM ILL i1, ILL i2, INFO p " +
                        "WHERE i1.id = p.id AND i2.id = p.id AND i1.disease <> i2.disease"
        );

        // Execute via SqlFieldsQuery
        IgniteConfiguration config = IgniteUtils.createIgniteConfig(addresses, true);
        long[] avgTimes = new long[queries.size()];
        long[] times;
        long before;
        try (Ignite client = Ignition.start(config)) {

            IgniteCache cache = client.cache("SQL_PUBLIC_INFO");
            SqlFieldsQuery fieldsQuery;

            // Each query is executed 10 times
            System.out.println("Executing " + queries.size() + " queries ...");
            for (String q : queries) {
                times = new long[10];
                fieldsQuery = new SqlFieldsQuery(q);
                fieldsQuery.setDistributedJoins(true);
                fieldsQuery.setCollocated(true);

                // Execution
                System.out.println("Executing query: " + q);
                for (int i = 0; i < 10; i++) {
                    before = System.nanoTime();
                    List<List<?>> result = cache.query(fieldsQuery).getAll();
                    times[i] = System.nanoTime() - before;
                }

                // Avg. Time
                avgTimes[queries.indexOf(q)] = SQLQueryUtils.avg(times);
                System.out.println("Avg. Time=" + avgTimes[queries.indexOf(q)] / 1000000000.0 + "s");
            }
        }
    }


    /**
     * Print query metrics for the with query generalization flexibly answered queries.
     *
     * @param address          Address string for the cluster (JDBC)
     * @param affinityFunction Clustering affinity function
     * @throws JSQLParserException    Error when parsing or generalizing the sample queries
     * @throws ClassNotFoundException Thrown when Ignite's JDBC driver was not found
     * @throws SQLException           Error when parsing or executing the generalized queries
     */
    public static void printFlexibleAnsweringMetrics(String address, ClusteringAffinityFunction affinityFunction)
            throws JSQLParserException, ClassNotFoundException, SQLException {

        // Queries
        List<String> queries = Arrays.asList(
                "SELECT p.Name, p.Age FROM ILL i, INFO p WHERE i.id = p.id AND i.disease = 'Liver Failure'",
                "SELECT p.Name, p.Age FROM ILL i, INFO p WHERE i.id = p.id AND i.disease = 'Liver Failure' " +
                        "AND p.AGE < 50",
                "SELECT p.name, p.age, p.address FROM ILL i1, ILL i2, INFO p WHERE i1.id = p.id " +
                        "AND i2.id = p.id AND i1.disease='Liver Failure' AND i2.disease='Hemoptysis'"
        );


        // Register driver & get connection
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + address + ";distributedJoins=true;" +
                "collocated=true");
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(QUERY_TIMEOUT);

        // Generalize & execute each query ..
        for (String q : queries) {

            // Generalize the query
            System.out.println("-------------------------------------------------------------------------------------");
            String generalized = FlexibleQueryAnswering.generalize(q, affinityFunction);
            System.out.println("Executing Query: " + q + "\nGeneralized Query: " + generalized);

            // Execute 10 times
            long[] times = new long[10];
            long before;
            for (int i = 0; i < 10; i++) {
                before = System.nanoTime();
                ResultSet resultSet = stmt.executeQuery(generalized);
                List<List<?>> allTuples = SQLQueryUtils.fetchAll(resultSet);
                times[i] = System.nanoTime() - before;
            }

            // Avg
            long avg = SQLQueryUtils.avg(times);
            System.out.println("Avg. Time: " + avg / 1000000000.0 + "s");
        }


    }


    /**
     * Test unit
     *
     * @param args
     * @throws ClassNotFoundException Thrown when Ignite's JDBC driver was not found
     * @throws SQLException           Error when parsing or executing the generalized queries
     * @throws JSQLParserException    Error when parsing or generalizing the sample queries
     * @throws SimilarityException    Error upon similarity function usage
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException, JSQLParserException, SimilarityException {
        // TODO
        printJDBCMetrics("141.5.107.8");
        printFieldsQueryMetrics(Arrays.asList("141.5.107.8:47500..47509", "141.5.107.75:47500..47509",
                "141.5.107.76:47500..47509"));

        String separ = File.separator;
        String termsFile = "csv" + separ + "terms100.txt";  // TODO must coincide with actual term set
        String simFile = "csv" + separ + "result100.csv";
        ClusteringAffinityFunction affinityFunction = new ClusteringAffinityFunction(0.12, termsFile, simFile);
        printFlexibleAnsweringMetrics("141.5.107.8", affinityFunction); //TODO

    }
}
