package utils;

import java.sql.*;

/**
 * This class provides a basic functionality to query the cluster for statistics.
 */
public class MetricsSQLIgnite {


    /**
     * Connect to the Ignite cluster, query for statistics and print them.
     * @param args Takes one JDBC IP address string of the cluster
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        if (args.length == 0) {
            throw new IllegalArgumentException("Call main method with one command line argument " +
                    "which is the JDBC IP address string for the Ignite cluster!");
        }

        // Register driver
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Return connection to the cluster (Port 10800 default for JDBC client)
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + args[0] + "/ignite");

        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(
            "SELECT NODE_ID, TOTAL_CPU, CUR_CPU_LOAD * 100, AVG_CPU_LOAD * 100," +
                    " HEAP_MEMORY_USED, HEAP_MEMORY_MAX, HEAP_MEMORY_TOTAL," +
                    " NONHEAP_MEMORY_USED, NONHEAP_MEMORY_MAX, NONHEAP_MEMORY_TOTAL " +
                "FROM NODE_METRICS " +
                "WHERE NODE_ID NOT IN " +
                "   (SELECT ID FROM NODES WHERE IS_CLIENT = 1 )");
        SQLQueryUtils.printResultSet(res);

        stmt.close();
        conn.close();
    }

}
