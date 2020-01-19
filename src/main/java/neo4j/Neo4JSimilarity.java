package neo4j;

import clusteringbasedfragmentation.similarityfunctions.MeSHSimilarityFunction;
import clusteringbasedfragmentation.SimilarityException;
import org.apache.commons.cli.*;
import org.apache.jena.ext.com.google.common.collect.ImmutableSet;
import org.apache.jena.ext.com.google.common.collect.Iterables;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.*;

import java.io.*;
import java.sql.Statement;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class provides the similarity function functionality for the usage of Ignite
 * together with a Neo4J database containing the MeSH terms and providing the similarity
 * calculation between the terms as one divided by the length of the shortest path
 * between two terms.
 */
public class Neo4JSimilarity implements MeSHSimilarityFunction, Serializable {


    private static final long serialVersionUID = 410875681206734027L;


    /**
     * Map for caching path lengths used for pairwise pathLengths of MeSH (disease) terms:
     * - key = term1 + '+' + term2   (lexicographically ordered)
     * - value = length(shortestPath(term1, term2))
     */
    private HashMap<String, Integer> pathLengths;

    /**
     * Set of terms.
     */
    private TreeSet<String> terms;

    /**
     * Caches terms (=keys) with their identifiers (=values).
     */
    private TreeMap<String, String> identifiers;

    /**
     * Neo4J DB IP address (opt. with port)
     */
    private String ipAddress;

    /**
     * Neo4J User.
     */
    private String user;

    /**
     * Neo4J Password.
     */
    private String password;


    /**
     * Initialize Neo4J MeSH connector.
     *
     * @param ipAddress Database ip of the form www.xxx.yyy.zzz, optional with port number
     * @param user      Neo4J user name
     * @param password  Neo4J user password
     * @throws SQLException
     */
    public Neo4JSimilarity(String ipAddress, String user, String password) throws SQLException {
        this(ipAddress, user, password, (Set<String>) null);
    }


    /**
     * Initialize Neo4J MeSH connector with explicitly fixed term set.
     *
     * @param ipAddress Database ip of the form www.xxx.yyy.zzz, optional with port number
     * @param user      Neo4J user name
     * @param password  Neo4J user password
     * @param terms     Predefined fixed set of terms
     * @throws SQLException
     */
    public Neo4JSimilarity(String ipAddress, String user, String password, Set<String> terms) throws SQLException {

        // Neo4J Params
        this.ipAddress = ipAddress;
        this.user = user;
        this.password = password;

        // Cache
        this.pathLengths = new HashMap<>();

        // Term set + identifiers map (needs to be fetched once from Neo4J if not provided explicitly)
        if (terms != null)
            this.terms = new TreeSet<>(terms);
        else
            getTerms();
        this.identifiers = null;

        // Initialize pathLength map for all pairs of terms
        this.initPathLengths(this.terms);
    }


    /**
     * Initialize Neo4J MeSH connector with explicitly fixed term set loaded from file.
     *
     * @param ipAddress Database ip of the form www.xxx.yyy.zzz, optional with port number
     * @param user      Neo4J user name
     * @param password  Neo4J user password
     * @param termsFile Path to file containing a predefined fixed set of terms (one term per line)
     * @throws SQLException
     */
    public Neo4JSimilarity(String ipAddress, String user, String password, String termsFile) throws SQLException {

        // Neo4J Params
        this.ipAddress = ipAddress;
        this.user = user;
        this.password = password;

        // Init terms from file
        String line;
        this.terms = new TreeSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(termsFile))) {
            while ((line = reader.readLine()) != null) {
                terms.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.identifiers = null;

        // Initialize pathLength map for all pairs of terms
        this.pathLengths = new HashMap<>();
        this.initPathLengths(this.terms);
    }


    /**
     * Initialize Neo4J MeSH connector with precalculated pairwise shortest path lengths between disease terms.
     *
     * @param ipAddress   Database ip of the form www.xxx.yyy.zzz, optional with port number
     * @param user        Neo4J user name
     * @param password    Neo4J user password
     * @param pathLengths HashMap containing shortest path lengths between diseases for similarity calculation
     *                    (Key = term1+term2, Value = pathLength)
     */
    public Neo4JSimilarity(String ipAddress, String user, String password, HashMap<String, Integer> pathLengths) {

        // Neo4J Params
        this.ipAddress = ipAddress;
        this.user = user;
        this.password = password;

        // Cache
        this.pathLengths = pathLengths;

        // Derive term set from path length cache key
        this.terms = (TreeSet<String>) pathLengths.keySet()
                .stream()
                .map(k -> k.split("\\+")[0])
                .collect(Collectors.toSet());

        // identifiers not known (yet), path length initialization not needed here
        this.identifiers = null;
    }


    /**
     * Calculates the similarity of two MeSH (disease) terms by querying the Neo4J-DB for the shortest
     * path between both.
     *
     * @param term1 A MeSH Term
     * @param term2 A MeSH Term
     * @return Similarity of the MeSH Terms
     * @throws SimilarityException Thrown if any exception occurred during similarity calculation
     */
    @Override
    public double similarity(String term1, String term2) throws SimilarityException {

        // Compare the terms
        int z = term1.compareTo(term2);
        if (z == 0)     // Equal terms?
            return 1.0;

        // Hashmap key is combined from both terms connected via a '+' char (lexicographically ordered)
        Integer pathLength;
        if (z < 0)
            pathLength = pathLengths.get(term1 + "+" + term2);
        else
            pathLength = pathLengths.get(term2 + "+" + term1);

        // Already cached? If yes, then return value from hashmap
        if (pathLength != null)
            return 1.0 / pathLength;

        // Else obtain the similarity from the Neo4J-DB with the following Cypher-Query
        StringBuilder cypherBuilder = new StringBuilder();
        cypherBuilder.append("MATCH (n:`http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor`) ")
                .append("WHERE n.`http://www.w3.org/2000/01/rdf-schema#label`=\"")
                .append(term1).append("\" WITH n\n")
                .append("MATCH (m:`http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor`) ")
                .append("WHERE m.`http://www.w3.org/2000/01/rdf-schema#label`=\"")
                .append(term2).append("\" WITH n,m\n")
                .append("MATCH p = shortestPath((n)-[*]-(m)) WHERE ALL(rel in relationships(p) ")
                .append("WHERE type(rel) in [\"http://id.nlm.nih.gov/mesh/vocab#broaderDescriptor\"])\n")
                .append("RETURN length(p);");

        // Querying
        double sim;
        try (Connection conn = DriverManager.getConnection("jdbc:neo4j:bolt://" + ipAddress, user, password)) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(cypherBuilder.toString());
            if (rs.next()) {
                pathLength = rs.getInt("length(p)");
                if (pathLength == 0) {
                    sim = 0.0;
                } else
                    sim = 1.0 / pathLength;
            } else {
                // No row returned (some of the terms did not match the label of a TopicalDescriptor)
                sim = -1.0;
                pathLength = -1;
            }
            stmt.close();
        } catch (SQLException e) {
            throw new SimilarityException("An SQLException occured:\nSQLState: " + e.getSQLState() + "\n"
                    + "Message: " + e.getMessage());
        }

        // Cache the shortest path length for the term pair (lexicographically ordered) and return the similarity value
        if (z < 0)
            pathLengths.put(term1 + "+" + term2, pathLength);
        else
            pathLengths.put(term2 + "+" + term1, pathLength);
        return sim;
    }

    /**
     * Get all terms and their identifiers if no concrete term set was provided to the constructor explicitly.
     * If a concrete term set was specified, then only return these terms with their identifiers
     * (Note: CUIs are not available, so use http://id.nlm.nih.gov/mesh/vocab#identifier instead)
     *
     * @return HashMap with keys=terms, values=identifiers, or null on an error
     */
    @Override
    public Map<String, String> getTermsWithCUIs() {

        // Already cached?
        if (identifiers != null && !identifiers.isEmpty())
            return identifiers;

        // Term set needs to be present (if not specified or empty, fetch all terms from neo4j)
        if (terms == null || terms.isEmpty())
            getTerms();

        // Query Neo4J-DB for all terms and identifiers
        identifiers = new TreeMap<>();
        StringBuilder cypherBuilder = new StringBuilder();
        cypherBuilder.append("MATCH (n:`http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor`)")
                .append("-[:`http://id.nlm.nih.gov/mesh/vocab#treeNumber`]->(t) ")
                .append("WHERE t.`http://www.w3.org/2000/01/rdf-schema#label` STARTS WITH \"C\"\n")
                .append("RETURN DISTINCT n.`http://www.w3.org/2000/01/rdf-schema#label`, ")
                .append("n.`http://id.nlm.nih.gov/mesh/vocab#identifier`;");

        // Query for terms
        try (Connection conn = DriverManager.getConnection("jdbc:neo4j:bolt://" + ipAddress, user, password)) {
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(cypherBuilder.toString());
            while (res.next()) {
                // Filter for those terms contained in the term set
                String term = res.getString(1);
                if (terms.contains(term))
                    identifiers.put(term, res.getString(2));
            }
        } catch (SQLException e) {
            System.err.println("An error occurred while trying to fetch all terms and identifiers from Neo4J:\n"
                    + e.getMessage());
            return null;
        }

        return identifiers;
    }

    /**
     * Get a set of all terms. Fetches all terms from Neo4J if there was not given a term set to the constructor
     * explicitly, and caches the term set.
     *
     * @return Term set, or null on an error
     */
    @Override
    public Set<String> getTerms() {

        // Already cached all terms?
        if (terms != null && !terms.isEmpty())
            return terms;

        // Query Neo4J-DB for ALL terms
        terms = new TreeSet<>();
        StringBuilder cypherBuilder = new StringBuilder();
        cypherBuilder.append("MATCH (n:`http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor`)")
                .append("-[:`http://id.nlm.nih.gov/mesh/vocab#treeNumber`]->(t) ")
                .append("WHERE t.`http://www.w3.org/2000/01/rdf-schema#label` STARTS WITH \"C\"\n")
                .append("RETURN DISTINCT n.`http://www.w3.org/2000/01/rdf-schema#label`;");

        // Query for terms
        try (Connection conn = DriverManager.getConnection("jdbc:neo4j:bolt://" + ipAddress, user, password)) {
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(cypherBuilder.toString());
            while (res.next()) {
                terms.add(res.getString(1));
            }
        } catch (SQLException e) {
            System.err.println("An error occurred while trying to fetch all terms from Neo4J:\n" + e.getMessage());
            return null;
        }

        return terms;
    }


    /**
     * For all pairs of terms query Neo4J for the length of the shortest path between them and store the value inside
     * the pathLenghts map
     *
     * @param terms Set of terms
     */
    public void initPathLengths(Set<String> terms) {

        // Cypher Query
        StringBuilder cypher = new StringBuilder();


        // Add all (used) terms in WITH statement (Note: terms are ordered and so will the list in WITH statement be)
        // Also: Some terms contain special characters that need to be escaped or embedded in double quotes or backticks
        cypher.append("WITH [\"")
                .append(String.join("\",\"", terms))
                .append("\"] AS terms\n");

        // Match all the terms to their TopicalDescriptors
        cypher.append("UNWIND terms as t\n")
                .append("MATCH (n:`http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor`) WHERE ")
                .append("n.`http://www.w3.org/2000/01/rdf-schema#label`=t\n")
                .append("WITH collect(n) as topics\n");

        // UNWIND twice (nested for loop) and get shortest path length for all pairs of terms (without inverse pairs!)
        // and return the result (term1, term2, shortest path length)
        cypher.append("UNWIND topics as n\n")
                .append("UNWIND topics as m\n")
                .append("WITH * WHERE n.`http://www.w3.org/2000/01/rdf-schema#label` < ")
                .append("m.`http://www.w3.org/2000/01/rdf-schema#label`\n")
                .append("MATCH p = shortestPath((n)-[*]-(m)) WHERE ALL(rel in relationships(p) WHERE type(rel) in ")
                .append("[\"http://id.nlm.nih.gov/mesh/vocab#broaderDescriptor\"])\n")
                .append("RETURN n.`http://www.w3.org/2000/01/rdf-schema#label`, ")
                .append("m.`http://www.w3.org/2000/01/rdf-schema#label`, length(p);");

        // Execute via driver API which allows for streaming results (to avoid memory overflow)
        try (Driver driver = GraphDatabase.driver("bolt://" + ipAddress, AuthTokens.basic(user, password))) {
            // Get a session
            Session session = driver.session(AccessMode.READ);

            // Execute and get the result stream
            Stream<Record> stream = session.run(cypher.toString()).stream();

            // Collect all results in a map: key = term1+term2, value = similarity value
            pathLengths = (HashMap<String, Integer>) stream.collect(Collectors.toMap(
//                    r -> r.get(0).asString() + "+" + r.get(1).asString(),
//                    r -> r.get(2).asInt())
                    r -> r.get(0).asString() + "+" + r.get(1).asString(),
                    r -> r.get(2).asInt()
            ));
            System.out.println("Finished initialization (cached " + pathLengths.size() + " path lengths)!");
            session.close();
        }

    }


    /**
     * Test unit that tests the connection to the Neo4J-DB, returns all the terms and finally
     * calculates some pairwise similarity values.
     * Also measures the time to fetch the path lengths for a set of terms from the database.
     *
     * @param args Mandatory: -u <username> or --user <username>, -p <password> or --password <password>,
     *             -i <ip> or --ipaddress <ip>
     *             Set -h or --help for usage information.
     * @throws Exception CLI-Parsing- or DB-related exceptions
     */
    public static void main(String... args) throws Exception {

        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Command line options (arguments):
        Options options = new Options();
        options.addOption("u", "user", true, "Specify user name");
        options.addOption("p", "password", true, "Sepcify user password");
        options.addOption("i", "ipaddress", true, "Specify ip address (opt. port)");
        options.addOption("h", "help", false, "Usage help");

        // Parse the commandline arguments (throws exception for any unrecognized argument or on other errors)
        CommandLineParser cmdParser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        try {
            cmd = cmdParser.parse(options, args, false);
        } catch (ParseException e) {
            System.err.println("Could not parse: " + e.getMessage());
            formatter.printHelp("SiFAMIS-Neo4J", options);
            return;
        }

        // Help needed?
        if (cmd.hasOption("h")) {
            formatter.printHelp("SiFAMIS-Neo4J", options);
            return;
        }

        // Check if username and password are set
        if (!cmd.hasOption("u")) {
            System.err.println("Please specify a username with -u <name> or --user <name>");
            return;
        }
        if (!cmd.hasOption("p")) {
            System.err.println("Please specify a password with -p <password> or --password <password>");
            return;
        }
        if (!cmd.hasOption("i")) {
            System.err.println("Please specify an ip address (opt. with a port) with -i <ip> or --ipaddress <ip>");
            return;
        }

        // Get ip, username and password
        String ip = cmd.getOptionValue("i");
        String user = cmd.getOptionValue("u");
        String pw = cmd.getOptionValue("p");
        Neo4JSimilarity neo = new Neo4JSimilarity(ip, user, pw);

        // Test terms and identifiers
        Map<String, String> map = neo.getTermsWithCUIs();
        for (String term : map.keySet()) {
            System.out.print(term + "(" + map.get(term) + "), ");
        }
        System.out.println("\n" + map.size() + " terms");


        // Measure run time
        long before = System.nanoTime();
        Set<String> terms = neo.getTerms();
        long diff = System.nanoTime() - before;
        System.out.println("It took " + diff / 1000000000.0 + "s to fetch all terms (JDBC)");

        // Test some similarities
        System.out.println("Similarity of Cough and Astmhma: " + neo.similarity("Cough", "Asthma"));
        System.out.println("Similarity of Cough and Cough: " + neo.similarity("Cough", "Cough"));
        System.out.println("Similarity of Asthma and Cough: " + neo.similarity("Asthma", "Cough"));
        System.out.println("Similarity of Blackwater Fever and Eye Abnormalities: "
                + neo.similarity("Blackwater Fever", "Eye Abnormalities"));
        System.out.println("Similarity of Blackwater Fever and Abnormalities, Eye: "
                + neo.similarity("Blackwater Fever", "Abnormalities, Eye"));


        // Test bulk similarity
        Set<String> sample = ImmutableSet.copyOf(Iterables.limit(terms, 200));
        before = System.nanoTime();
        neo.initPathLengths(sample);
        diff = System.nanoTime() - before;
        System.out.println("It took " + diff / 1000000000.0 + "s to calculate " +
                (sample.size() * (sample.size() - 1) / 2) + " path lengths (bulk)");
        for (String key : neo.pathLengths.keySet()) {
            System.out.println(key + " = " + neo.pathLengths.get(key));
        }

    }
}
