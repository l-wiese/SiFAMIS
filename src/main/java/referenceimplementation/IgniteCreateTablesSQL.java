package referenceimplementation;

import com.github.javafaker.Faker;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Random;

/**
 * This class provides the functionality to connect to the ignite cluster via JDBC. The required tables are created with
 * SQL CREATE statements and filled with data with SQL INSERT statements. Here, no clustering-based fragmentation is
 * used, only Apache Ignite's own mechanism, affinity collocation, is used to enable better performance for queries.
 * Date fragmentation is handled by Ignite itself.
 */
public class IgniteCreateTablesSQL {

    /**
     * Default number of INFO-Tuples that will be generated and inserted.
     */
    private static final int DFLT_NUMBER_INFO_TUPLES = 50;

    /**
     * Default number of ILL-Tuples that will be generated and inserted.
     */
    private static final int DFLT_NUMBER_ILL_TUPLES = 100;


    /**
     * Ip Addresses of the cluster
     */
    private static String ADDRESSES = "141.5.107.8, 141.5.107.75, 141.5.107.76";    // TODO


    /**
     * Main method. Connects to the cluster, drops and creates tables via SQL statements and fills them finally with
     * randomized data.
     *
     * @param args See comments in main method or use -h or --help for getting usage information.
     */
    public static void main(String[] args) {

        // cmd line options: -h or --help for usage info, -sf or --scaling factor (is multiplied with default number
        // of persons and diseases) followed by a non-negative integer (optional, default value is 0 (= no tuples))
        Options options = new Options();
        options.addOption("h", "help", false, "Show help message");
        Option scalingf = new Option("sf", "scalingFactor", true,
                "Scaling factor (SF) for DDB size (non-negative integer)");
        scalingf.setArgName("SF");
        scalingf.setArgs(1);
        scalingf.setType(Number.class);
        options.addOption(scalingf);

        // Parse cmd line args
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args, false);
        } catch (ParseException e) {
            System.err.println("Error when parsing command line input:\n" + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("CLITester", options);
            System.exit(-1);
        }

        // Help?
        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("CLITester", options);
            System.exit(0);
        }

        // Scaling factor defined? (default is 0)
        int sf = 0;
        if (cmd.hasOption("sf")) {
            try {
                sf = (Integer) cmd.getParsedOptionValue("sf");
                if (sf < 0)
                    throw new ParseException("Scaling factor must be a non-negative integer!");
            } catch (ClassCastException | ParseException e) {
                System.err.println("Could not parse scaling factor!\n" + e.getMessage());
                System.exit(-1);
            }
        }

        // Connect and fill tables
        connectAndFill(ADDRESSES, DFLT_NUMBER_INFO_TUPLES * sf, DFLT_NUMBER_ILL_TUPLES * sf,
                true, true);

    }


    /**
     * Load the term set for this implementation.
     *
     * @return Term set
     */
    private static ArrayList<String> loadTerms() {
        // Load terms
        String line;
        String separ = File.separator;
        String termsFile = "csv" + separ + "terms100.txt";
        ArrayList<String> terms = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(termsFile))) {
            while ((line = reader.readLine()) != null) {
                terms.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return terms;
    }


    /**
     * Connect to the database, drop & create tables and fill with data
     *
     * @param addresses      IP Addresses
     * @param infos          Number of INFO tuples (persons)
     * @param ills           Number of ILL tuples (diseases)
     * @param recreateTables If the tables are already created, then they will be recreated (DROP+CREATE) if this
     *                       flag is set to true. Otherwise, if the tables already exist, they will not be recreated
     * @param clearTables    If set to true, then all the tables content will be cleared. If this is set to false,
     *                       and recreateTables-flag is set to true, the tables will also be cleared.
     */
    public static void connectAndFill(String addresses, long infos, long ills, boolean recreateTables,
                                      boolean clearTables) {
        // Load terms
        ArrayList<String> terms = loadTerms();

        // Get the connection
        Connection conn = null;
        Statement stmt = null;
        long start, diff;
        System.out.println("Initializing connection to the cluster ...");
        try {
            // Initialize connection and statement
            conn = getConnection(addresses);
            System.out.println("Connected to the cluster!");
            stmt = conn.createStatement();

            // Create and Fill the tables
            // If tables shall be cleared then just drop them and recreate (as TRUNCATE is not supported atm,
            // this is said to be fastest...)
            System.out.println("Creating tables ...");
            start = System.nanoTime();
            if (recreateTables || clearTables)
                createTables(conn, true);
            else
                createTables(conn, false);
            diff = System.nanoTime() - start;
            System.out.println("Created tables in " + diff / 1000000.0 + "ms!");

            if (infos > 0 || ills > 0) {
                System.out.println("Filling tables with " + infos + " INFO-Tuples and " + ills + " ILL-Tuples ...");
                start = System.nanoTime();
                fillTables(conn, terms, infos, ills);
                diff = System.nanoTime() - start;
                System.out.println("Filled tables in " + diff / 1000000.0 + "ms!");
            }
            stmt.close();
            conn.close();
            System.out.println("Disconnected!");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    /**
     * Fills the tables with randomly generated data
     *
     * @param conn  Connection to the cluster
     * @param terms List of MeSH terms used here
     * @param p     Maximum number of different persons that are generated (might be less persons than this value)
     * @param d     Defines how many Ill tuples are generated for maximal p persons and stored in cluster
     * @throws SQLException Error when parsing or executing SQL queries.
     */
    private static void fillTables(Connection conn, ArrayList<String> terms, long p, long d) throws SQLException {
        // Statements for table insertions
        String insertIll, insertInfo, insertTreat;
        insertIll = "INSERT INTO ILL (ID, Disease, CUI) VALUES (?, ?, ?)";
        insertInfo = "INSERT INTO INFO (ID, Name, Address, Age) VALUES (?, ?, ?, ?)";
        insertTreat = "INSERT INTO TREAT (ID, Prescription, Success) VALUES (?, ?, ?)";
        PreparedStatement prepIll, prepInfo, prepTreat;
        prepIll = conn.prepareStatement(insertIll);
        prepInfo = conn.prepareStatement(insertInfo);
        prepTreat = conn.prepareStatement(insertTreat);


        // Store p random persons (Info objects) with array index is id
        InfoGenerator generator = new InfoGenerator(new Faker());
        for (int i = 0; i < p; i++) {
            Info person = generator.generate(i);
            prepInfo.setInt(1, person.getId());
            prepInfo.setString(2, person.getName());
            prepInfo.setString(3, person.getAddress());
            prepInfo.setInt(4, person.getAge());
            prepInfo.executeUpdate();
            System.out.println("INFO: " + i * 100.0 / p + "%");
        }

        // Store d random disease entries (Ill objects) and treat entries (Treat objects)
        int numterms = terms.size();
        Random r = new Random(System.nanoTime());
        for (int i = 0; i < d; i++) {
            // Some random disease for a random personID (bound by argument p) and some prescription
            String disease = terms.get(r.nextInt(numterms));
            int personID = (new Random()).nextInt((int) p);

            // Add it
            prepIll.setInt(1, personID);
            prepIll.setString(2, disease);
            prepIll.setInt(3, 123456789);
            prepTreat.setInt(1, personID);
            prepTreat.setString(2, "Prescr.XY.01");
            prepTreat.setBoolean(3, true);
            try {
                prepIll.executeUpdate();
                prepTreat.executeUpdate();
            } catch (SQLException e) {
                continue;
            }

            System.out.println("ILL: " + i * 100.0 / d + "%");
        }


    }


    /**
     * Drops tables if they exist and creates the partitioned and collocated tables (Ill, Info) without any backups.
     *
     * @param conn           Connection to the database
     * @param recreateTables If this is true, the tables will be dropped and recreated!
     * @throws SQLException Error when parsing or executing the DROP and CREATE SQL queries.
     */
    private static void createTables(Connection conn, boolean recreateTables) throws SQLException {
        // Statement
        Statement stmt = conn.createStatement();

        // DROP tables Ill, Info and Treat
        String dropStmt = "DROP TABLE IF EXISTS ILL; DROP TABLE IF EXISTS INFO; DROP TABLE IF EXISTS TREAT";
        if (recreateTables)
            stmt.executeUpdate(dropStmt);

        // CREATE tables Ill, Info and Treat
        String createStmt = "CREATE TABLE IF NOT EXISTS ILL  (ID INT, Disease VARCHAR, CUI VARCHAR, " +
                "PRIMARY KEY (ID, Disease)) WITH \"template=partitioned,backups=0,affinityKey=ID\"";
        stmt.executeUpdate(createStmt);

        createStmt = "CREATE TABLE IF NOT EXISTS INFO (ID INT PRIMARY KEY, Name VARCHAR, Address VARCHAR, Age INT) " +
                "WITH \"template=partitioned,backups=0,affinityKey=ID\"";
        stmt.executeUpdate(createStmt);

        createStmt = "CREATE TABLE IF NOT EXISTS TREAT (ID INT, Prescription VARCHAR, Success BOOLEAN, " +
                "PRIMARY KEY (ID, Prescription)) WITH \"template=partitioned,backups=0,affinityKey=ID\"";
        stmt.execute(createStmt);
    }


    /**
     * Registers driver and returns connection to the cluster
     *
     * @param addresses Comma-separated ip addresses of cluster nodes (e.g. 192.168.2.1, 192.168.2.2, ...), optionally
     *                  port (ranges) may be included
     * @return Connection
     * @throws ClassNotFoundException Thrown when Ignite's JDBC driver was not found
     * @throws SQLException           Error when parsing or executing SQL queries.
     */
    private static Connection getConnection(String addresses) throws ClassNotFoundException, SQLException {
        // Register driver
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Return connection to the cluster (Port 10800 default for JDBC client)
        return DriverManager.getConnection("jdbc:ignite:thin://" + addresses + ";" +
                "distributedJoins=true");
    }

}
