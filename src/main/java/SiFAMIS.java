import clusteringbasedfragmentation.*;
import clusteringbasedfragmentation.similarityfunctions.MeSHSimilarityFunction;
import clusteringbasedfragmentation.similarityfunctions.SimClusteringTableSimilarity;
import clusteringbasedfragmentation.similarityfunctions.SimTableSimilarity;
import neo4j.Neo4JSimilarity;
import neo4j.PathLengthCSV;
import org.apache.commons.cli.*;
import referenceimplementation.IgniteCreateTablesSQL;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Arrays;


/**
 * This is the main class of the project. It is used via command line and provides a CLI to set up the
 * distributed database and the clustering.
 * NOTE: The via command line given arguments are used as configuration for the clients started from
 * this class and must correspond to the chosen XML-configuration file contents. Otherwise strange
 * behavior may occur!.
 */
public class SiFAMIS {

    private static final int DFLT_NUM_PERSONS = 50;
    private static final int DFLT_NUM_DISEASES = 100;
    private static final int DFLT_SF = 0;
    private static final int ALL_TERMS_NEO4J = 0;
    private static final Integer[] ALLOWED_TERM_NUMS = {0, 10, 30, 100, 500, 1000, 2500};


    /**
     * CLI options.
     */
    private Options options;

    /**
     * Scaling factor.
     */
    private long sf;

    /**
     * Similarity threshold alpha.
     */
    private double alpha;

    /**
     * Size of term set (0, 10, 30, 100, 500, 1000, 2500 are supported, see ALLOWED_TERM_NUMS array).
     */
    private int numTerms;

    /**
     * Ip addresses of the cluster nodes (optionally with port (range))
     */
    private String[] addresses;


    /**
     * Flag indicating whether all tables shall be recreated. If true, then DROP+CREATE is applied, else only CREATE.
     */
    private boolean recreateTables;

    /**
     * Flag indicating whether all tables shall be cleared before insertion. If true, then tables are truncated,
     * else nothing happens.
     */
    private boolean clearTables;

    /**
     * Flag indicating whether similarity ignite cache shall be initialized
     * (= created and filled with data).
     */
    private boolean initSimCache;

    /**
     * Flag indicating whether clustering ignite cache shall be initialized
     * (= created and filled with data).
     */
    private boolean initClusteringCache;


    /**
     * Flag indicating whether neo4j MeSH DB shall be used.
     */
    private boolean neo4jEnabled;

    /**
     * Ip Address (opt. port) of neo4j db instance.
     */
    private String neo4jAddress;

    /**
     * User name of neo4j.
     */
    private String neo4jUser;

    /**
     * Password of neo4j.
     */
    private String neo4jPassword;

// ##################################### Constructor #########################################


    /**
     * Constructor, creates the CLI options for this object with help of the Apache Commons CLI package
     */
    private SiFAMIS() {
        // Command line options (arguments):
        options = new Options();

        // DDB Modi: (only one is allowed, if specified incorrectly strange behavior may occur)
        // -r or --reference for reference mode (reference "standard" implementation without clustering-based fragmentation)
        // -m or --materialized for materialized mode (materialized fragment approach)
        // -p or --partition for partition mode (partition number approach)
        Option reference, materialized, partition;
        reference = new Option("r", "reference", false, "Use reference mode");
        reference.setRequired(true);
        materialized = new Option("m", "materialized", false, "Use materialized mode");
        materialized.setRequired(true);
        partition = new Option("p", "partition", false, "Use partition mode");
        partition.setRequired(true);
        OptionGroup modi = new OptionGroup();
        modi.addOption(reference);
        modi.addOption(materialized);
        modi.addOption(partition);
        modi.setRequired(true);
        options.addOptionGroup(modi);

        // Use Neo4J-DB for similarities: (can be used with -t option to fix clustering to a specific size (termsfile))
        // -n or --neo4j followed by the 3 args: the ip address of the neo4j db instance, the user name and the password
        Option neo = new Option("n", "neo4j", true, "Use Neo4J MeSH Database for " +
                "calculating the similarities");
        neo.setArgs(3);
        options.addOption(neo);

        // Scaling factor (is multiplied with default number of persons and diseases)
        // -sf or --scalingFactor followed by a non-negative integer (optional, default value is 0 (= no tuples created)
        Option scalingf = new Option("sf", "scalingFactor", true,
                "Scaling factor (SF) for DDB size (non-negative integer)");
        scalingf.setArgName("SF");
        scalingf.setArgs(1);
        scalingf.setType(Number.class);
        options.addOption(scalingf);

        // IP address(es):
        // -ip or --addresses followed by a comma-separated list of ip addresses (optionally with port ranges)
        Option ips = new Option("ip", "addresses", true, "Comma-separated list of IP " +
                "addresses (optional with port (ranges) of cluster nodes, e.g. " +
                "127.0.0.1:12345,192.168.5.1:47500..47509)");
        ips.setRequired(true);
        ips.setArgName("IP ADDRESSES");
        ips.setArgs(1);
        options.addOption(ips);

        // Alpha (similarity threshold):
        // -a or --alpha followed by a double value that should be between 0.0 and 1.0
        Option alpha = new Option("a", "alpha", true, "Similarity-threshold alpha " +
                "(non-negative double value)");
        alpha.setRequired(true);
        alpha.setArgs(1);
        alpha.setArgName("ALPHA");
        alpha.setType(Double.class);
        options.addOption(alpha);

        // Term set size (0 (= all), rest see ALLOWED_TERM_NUMS array):
        // -t or --terms followed by integer value, this is optional if neo4j similarity is used, otherwise mandatory
        Option terms = new Option("t", "terms", true, "Term set size (" +
                Arrays.toString(ALLOWED_TERM_NUMS) + " are supported, 0 = all terms)");
        terms.setRequired(false);
        terms.setArgs(1);
        terms.setArgName("TERM SET SIZE");
        terms.setType(Number.class);
        options.addOption(terms);

        // Recreate tables (-rt or --recreateTables), clear tables (-ct or --clearTables) flags:
        options.addOption("rt", "recreateTables", false, "Recreate the tables before " +
                "filling with data");
        options.addOption("ct", "clearTables", false, "Clear the tables before filling " +
                "with data");

        // Init Similarity Table (SIM Cache, -is or --initSim)
        options.addOption("is", "initSim", false, "Init the similarity cache");

        // Init Clustering Table (CLUSTERING Cache, -ic or --initClu)
        options.addOption("ic", "initClu", false, "Init the clustering cache");


        // Help (Print usage): -h or --help
        Option help = new Option("h", "help", false, "Print usage information");
        options.addOption(help);
    }


    /**
     * This method processes the command line input by parsing it with the specified {@link Options} and checking
     * all option values for correctness and validity. After this, the via the options specified configuration is
     * used to start an Ignite client node (e.g. for data generation).
     * @param args Command line arguments passed to main method
     * @throws ParseException Thrown whenever an error occurs on parsing or checking
     * @throws SimilarityException If an exception occurs while calculating similarity
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException If a file could not be found/opened, etc.
     */
    private void processCommandLineInput(String[] args) throws ParseException, SQLException, ClassNotFoundException,
            SimilarityException, IOException {

        // Parse the commandline arguments (throws exception for any unrecognized argument or on other errors)
        CommandLineParser cmdParser = new DefaultParser();
        CommandLine cmd = cmdParser.parse(options, args, true);

        // Get values and check them for all the parsed options
        getAndCheckOptions(cmd);

        // File paths according to options
        String separ = File.separator;
        String termsFile = "csv" + separ + "terms" + numTerms + ".txt";
        String simFile = "csv" + separ + "result" + numTerms + ".csv";
        String pathLengthFile = "csv" + separ + "pathlengths";
        if (numTerms == 0)
            pathLengthFile += "All.csv";
        else
            pathLengthFile += numTerms + ".csv";
        String clusteringFile = "clustering" + separ + "clustering";
        if (numTerms == 0)
            clusteringFile += "All";
        else
            clusteringFile += numTerms;

        // Address string
        String addrString = String.join(",", addresses);

        // Recreate/clear tables and init similarity & clustering cache flags
        recreateTables = cmd.hasOption("rt");
        clearTables = cmd.hasOption("ct");
        initSimCache = cmd.hasOption("is");
        initClusteringCache = cmd.hasOption("ic");

        // Set similarity function for AffinityFunction if neo4j is enabled, otherwise load similarities from csv
        MeSHSimilarityFunction similarity = null;
        ClusteringAffinityFunction affinityFunction;
        if (neo4jEnabled) {

            // All terms or restricted, fixed term set (loaded from csv)
            if (numTerms == ALL_TERMS_NEO4J) {
                similarity = new Neo4JSimilarity(neo4jAddress, neo4jUser, neo4jPassword);
            } else {
                similarity = new Neo4JSimilarity(neo4jAddress, neo4jUser, neo4jPassword, termsFile);
            }

            affinityFunction = new ClusteringAffinityFunction(alpha, similarity);
        } else {
            // Choose the simple affinity functions if the term set size is small, else use the sim table approach
            if (numTerms == 10 || numTerms == 30 || numTerms == 100)
                affinityFunction = new ClusteringAffinityFunction(alpha, termsFile, simFile);
            else {
                // Distinguish between similarity cache only and similarity + clustering cache
                if (!initClusteringCache) {
                    similarity = new SimTableSimilarity("SIM",
                            PathLengthCSV.readTermSet(pathLengthFile), false);
                } else {
                    similarity = new SimClusteringTableSimilarity("SIM",
                            PathLengthCSV.readTermSet(pathLengthFile), false, "CLUSTERING");
                }
                affinityFunction = new ClusteringAffinityFunction(similarity, clusteringFile);
            }
        }



        // Now process according to the found configuration
        if (cmd.hasOption("r")) {               // reference mode

            IgniteCreateTablesSQL.connectAndFill(addrString, DFLT_NUM_PERSONS * sf,
                        DFLT_NUM_DISEASES * sf, recreateTables, clearTables);

        } else if (cmd.hasOption("m")) {        // materialized mode

            // Materialized fragments approach
            materializedfragments.SetupCaches setup = new materializedfragments.SetupCaches(affinityFunction,
                    Arrays.asList(addresses), DFLT_NUM_PERSONS * sf, DFLT_NUM_DISEASES * sf,
                    recreateTables, clearTables, initSimCache, initClusteringCache);
        } else if (cmd.hasOption("p")) {        // partitions mode

            // partition number approach
            partitionnumbers.SetupCaches setup = new partitionnumbers.SetupCaches(affinityFunction,
                    Arrays.asList(addresses), DFLT_NUM_PERSONS * sf, DFLT_NUM_DISEASES * sf,
                    recreateTables, clearTables, initSimCache, initClusteringCache);
        }

    }


    /**
     * This method gets all values from the list of arguments parsed against a {@link Options} descriptor and checks
     * all of them for correctness and validity.
     * @param cmd Parsed list of arguments
     * @throws ParseException Thrown whenever an error occurs on parsing or checking
     */
    private void getAndCheckOptions(CommandLine cmd) throws ParseException {

        // First check if help shall be printed
        if (cmd.hasOption("h")) {
            this.printUsageHelp();
            System.exit(0);
        }

        // Check if scaling factor was defined correctly, set default value if no factor was defined (optional arg)
        if (cmd.hasOption("sf")) {
            try {
                sf = new Long(cmd.getOptionValue("sf"));
            } catch (ClassCastException e) {
                throw new ParseException("Could not parse scaling factor!\n" + e.getMessage());
            }
            if (sf < 0)
                throw new ParseException("Scaling factor must be a non-negative integer!");
        } else {
            sf = DFLT_SF;
        }

        // Check if alpha (similarity threshold) was defined correctly
        try {
            alpha = new Double(cmd.getOptionValue("a"));
        } catch (ClassCastException e) {
            throw new ParseException("Could not parse similarity threshold alpha!\n" + e.getMessage());
        }
        if (alpha < 0.0)
            throw new ParseException("Alpha (similarity-threshold) must be a non-negative double!");

        // Check if neo4j shall be used
        neo4jEnabled = cmd.hasOption("n");
        if (neo4jEnabled) {
            // Get & check Neo4J option args
            String[] neo4jArgs = cmd.getOptionValues("n");
            neo4jAddress = checkIpAddressList(neo4jArgs[0])[0];
            neo4jUser = neo4jArgs[1];
            neo4jPassword = neo4jArgs[2];
        }

        // If specified, check if number of terms equals to one of the allowed term numbers in ALLOWED_TERM_NUMS.
        // If not specified, then Neo4J must be specified! Else, an exception is thrown ...
        if (cmd.hasOption("t")) {
            try {
                numTerms = new Integer(cmd.getOptionValue("t"));
            } catch (ClassCastException e) {
                throw new ParseException("Could not parse number of terms!\n" + e.getMessage());
            }
            if (!Arrays.asList(ALLOWED_TERM_NUMS).contains(numTerms))
                throw new ParseException("Number of terms must be equal to one of the following: "
                        + Arrays.toString(ALLOWED_TERM_NUMS)+ "! Your choice: " + numTerms);
        } else {
            numTerms = ALL_TERMS_NEO4J;
            if (!neo4jEnabled)
                throw new ParseException("Number of terms " + Arrays.toString(ALLOWED_TERM_NUMS) +  " must be " +
                        "specified unless Neo4J used (-n or --neo)!");
        }

        // Check the ip addresses list (should be comma-separated ip addresses, optional with port (ranges))
        String addrString = cmd.getOptionValue("ip");
        this.addresses = checkIpAddressList(addrString);     // throws an error if something is invalid or not parsable

    }


    /**
     * Split address string and checks the validity of all ip addresses (incl. port (range)).
     * @param addrString Ip Address List (comma-separated in one string)
     * @return Array of all IPs
     * @throws ParseException Thrown whenever an error occurs on parsing or checking
     */
    private String[] checkIpAddressList(String addrString) throws ParseException {

        // For each ip address in the list check it's validity and the validity of the
        // optionally specified port (range)
        String[] addresses = addrString.split(",");
        for (String ip : addresses) {

            // Port (range) specified?
            if (ip.contains(":")) {

                // validity check of ip address
                try {
                    InetAddress.getByName(ip.split(":")[0]);
                } catch (UnknownHostException e) {
                    throw new ParseException("Could not parse ip addresses!\n" + e.getMessage());
                }

                // validity check of port (range)
                String ports = ip.split(":")[1];
                if (ports.contains("..")) {

                    // Port range check
                    int p1, p2;
                    try {
                        p1 = new Integer(ports.split("\\.\\.")[0]);
                        p2 = new Integer(ports.split("\\.\\.")[1]);
                    } catch (NumberFormatException e) {
                        throw new ParseException("Could not parse ip addresses!\n" + e.getMessage());
                    }
                    if (p1 < 0 || p1 > 65535 || p2 < 0 || p2 > 65535) {
                        throw new ParseException("Invalid port range specified (" + ports + ")!");
                    }

                } else {
                    int p;
                    try {
                        p = new Integer(ports);
                    } catch (NumberFormatException e) {
                        throw new ParseException("Could not parse ip addresses!\n" + e.getMessage());
                    }
                    if (p < 0 || p > 65535) {
                        throw new ParseException("Invalid port range specified (" + ports + ")!");
                    }
                }

            } else {

                // No port (range) specified
                try {
                    InetAddress.getByName(ip);
                } catch (UnknownHostException e) {
                    throw new ParseException("Could not parse ip addresses!\n" + e.getMessage());
                }
            }
        }

        return addresses;
    }


    /**
     * Prints usage information to the options.
     */
    public void printUsageHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SiFAMIS", options);
    }



    /**
     * Entry point that is started from command line with arguments and provides interface to setup tables and fill them
     * with data
     * @param args Options as defined in the constructor {@link SiFAMIS#SiFAMIS()}
     * @throws SQLException Thrown upon connection (JDBC) or SQL errors
     * @throws ClassNotFoundException Thrown when JDBC driver could not be found
     */
    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        // Create SiFAMIS object that defines CLI options
        SiFAMIS sifamis = new SiFAMIS();

        // Process given cmd line args
        try  {
            sifamis.processCommandLineInput(args);
        } catch (ParseException | ClassCastException e) {
            // Error while parsing (e.g. unrecognized arg or required arg missing -> print error & usage info)
            System.err.println(e.getMessage());
            sifamis.printUsageHelp();
            return;
        } catch (SimilarityException e) {
            e.printStackTrace();
            System.err.println("A similarity exception occurred! " + e.getMessage());
            return;
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("An IO Exception occured!");
        }

    }
}
