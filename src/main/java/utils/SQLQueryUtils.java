package utils;

import clusteringbasedfragmentation.ClusteringAffinityFunction;
import clusteringbasedfragmentation.SimilarityException;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import org.apache.ignite.cache.query.FieldsQueryCursor;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * This class provides useful functionality for query/query result processing
 * and connection establishment.
 */
public class SQLQueryUtils {

    /**
     * Print the ResultSet of an executed query to stdout.
     *
     * @param res Result of the query
     */
    public static void printResultSet(ResultSet res) throws SQLException {
        ResultSetMetaData rsmd = res.getMetaData();
        System.out.println("### Result: " + rsmd.getColumnCount() + " columns");
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            System.out.print(rsmd.getColumnName(i) + "\t|\t");
        }
        System.out.println();
        while (res.next()) {
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                System.out.print(res.getString(i) + "\t|\t");
            }
            System.out.println();
        }
        System.out.println("################");
    }


    /**
     * Print the result set of a given {@link FieldsQueryCursor}.
     *
     * @param cursor Result of a {@link org.apache.ignite.cache.query.SqlFieldsQuery}
     */
    public static void printCursor(FieldsQueryCursor<List<?>> cursor) {

        for (int i = 0; i < cursor.getColumnsCount(); i++) {
            System.out.print(cursor.getFieldName(i) + "\t|\t");
        }
        System.out.println("\n------------------------------------------------------------------------------------");
        printCursorGetAll(cursor.getAll());
    }


    /**
     * Print the result of the result of {@link FieldsQueryCursor#getAll()},
     * i.e. a list of tuple rows.
     *
     * @param result List of tuple rows
     */
    public static void printCursorGetAll(List<List<?>> result) {
        for (List<?> row : result) {
            for (int i = 0; i < row.size(); i++) {
                System.out.print(row.get(i) + "\t|\t");
            }
            System.out.println();
        }
        System.out.println("------------------------------------------------------------------------------------");
    }


    /**
     * Get the partitions for all the given relaxation attribute selection conditions.
     *
     * @param selections relaxation attribute selections
     * @param aff        Affinity function to determine partitions according to cluster
     * @return Partition numbers
     */
    public static HashSet<Integer> getPartitionsForSelections(ArrayList<EqualsTo> selections, ClusteringAffinityFunction aff) {
        int[] partitions = new int[selections.size()];
        for (int i = 0; i < selections.size(); i++) {
            EqualsTo eq = selections.get(i);
            String term = ((StringValue) eq.getRightExpression()).getValue();
            try {
                partitions[i] = aff.identifyCluster(term);
            } catch (SimilarityException e) {
                e.printStackTrace();
                System.err.println("An error occured: " + e.getMessage());
            }
        }
        HashSet<Integer> result = new HashSet<>();
        for (int i : partitions)
            result.add(i);
        return result;
    }


    /**
     * Return the resulting tuples of a query (a {@link ResultSet}) in a nested {@link List},
     * i.e. a list of tuple rows, by iterating over the result set and fetching all single answers.
     *
     * @param result Result of a query
     * @return List of tuple rows
     * @throws SQLException
     */
    public static List<List<?>> fetchAll(ResultSet result) throws SQLException {

        if (result == null)
            return null;

        // Returns list of rows
        List<List<?>> all = new ArrayList<>();

        // Get column count
        ResultSetMetaData rsmd = result.getMetaData();
        int columnCount = rsmd.getColumnCount();

        // Store each tuple
        while (result.next()) {
            List<String> tuple = new ArrayList<>();
            for (int i = 1; i <= columnCount; ++i) {
                tuple.add(result.getString(i));
            }
            all.add(tuple);
        }

        return all;
    }


    /**
     * Calculates the average of the given long array
     *
     * @param times Array
     * @return Average
     */
    public static long avg(long[] times) {
        // Calculate average
        long sum = 0;
        for (long t : times)
            sum += t;
        return sum / (long) times.length;
    }

    /**
     * Calculates the average of the given long array
     *
     * @param times Array
     * @return Average
     */
    public static long avg(Long[] times) {
        // Calculate average
        long sum = 0;
        for (long t : times)
            sum += t;
        return sum / (long) times.length;
    }


    /**
     * Get a JDBC connection to the Ignite cluster
     *
     * @param address JDBC IP address String of the cluster
     * @return Connection to the Ignite cluster
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static Connection getConnection(String address) throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + address + ";distributedJoins=true");
        return conn;
    }


    /**
     * Escape single quotes in the given term (e.g. in "Sjogren's Syndrome") for the sql query
     * with two single quotes (i.e. "Sjogren''s Syndrome" is returned)
     *
     * @param term Term to escape
     * @return Escaped term
     */
    public static String escape(String term) {
        if (!term.contains("'"))
            return term;

        return term.replaceAll("'", "''");
    }

}
