package partitionnumbers;

import clusteringbasedfragmentation.ClusteringAffinityFunction;
import clusteringbasedfragmentation.similarityfunctions.SimClusteringTableSimilarity;
import clusteringbasedfragmentation.similarityfunctions.SimTableSimilarity;
import clusteringbasedfragmentation.SimilarityException;
import clusteringbasedfragmentation.similarityfunctions.SimilarityFunction;
import materializedfragments.Ill;
import materializedfragments.IllKey;
import materializedfragments.Info;
import materializedfragments.Treat;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import utils.IgniteUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;


/**
 * This class implements the functionality to setup the Ignite cluster's caches
 * and fill the created caches with data in the partition number approach.
 */
public class SetupCaches {

    /**
     * Ignite SQL Table Prefix (SQL_SCHEMANAME_...)
     */
    private static final String IGNITE_PREFIX = "SQL_PUBLIC_";

    /**
     * Affinity Function with clustering and partition mappings
     */
    private ClusteringAffinityFunction affinityFunction;


    /**
     * Client node
     */
    private Ignite client;

    /**
     * Ignite configuration for the client
     */
    private IgniteConfiguration clientConfig;

    /**
     * Info cache (Persons).
     */
    private IgniteCache infoCache;

    /**
     * Ill cache (Diseases).
     */
    private IgniteCache illCache;


    /**
     * Treat cache (Prescriptions).
     */
    private IgniteCache treatCache;


    /**
     * Similarity cache.
     */
    private IgniteCache simCache;


    /**
     * Clustering cache.
     */
    private IgniteCache clusteringCache;


// #################################### Constructors ##############################################


    /**
     * Constructor
     *
     * @param affinityFunction    Affinity Function for clustering-based fragmentation
     * @param addresses           Addresses of ignite nodes for DiscoverySpi
     * @param p                   Number of persons
     * @param d                   Number of diseases
     * @param recreateTables      If the tables are already created, then they will be recreated (DROP+CREATE) if this
     *                            flag is set to true. Otherwise, if the tables already exist, they will not be
     *                            recreated
     * @param clearTables         If set to true, then all the tables content will be cleared. If this is set to false,
     *                            and recreateTables-flag is set to true, the tables will also be cleared.
     * @param initSimCache        If set to true, then the similarity cache will be initialized
     * @param initClusteringCache If set to true, then the clustering cache will be created and initialized.
     */
    public SetupCaches(ClusteringAffinityFunction affinityFunction, Collection<String> addresses, long p, long d,
                       boolean recreateTables, boolean clearTables, boolean initSimCache, boolean initClusteringCache)
            throws SQLException, ClassNotFoundException {

        this.affinityFunction = affinityFunction;

        // Init Config
        this.clientConfig = IgniteUtils.createIgniteConfig(addresses, true);

        // Start the client
        this.client = Ignition.getOrStart(this.clientConfig);

        // Init sim & clustering cache
        if (initSimCache) {
            initSimCache();
        } else {
            this.simCache = null;
        }
        if (initClusteringCache) {
            initClusteringCache();
        } else {
            this.clusteringCache = null;
        }

        // Register driver
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Return connection to the cluster (Port 10800 default for JDBC client)
        String addressString = String.join(",", addresses);
        System.out.println("Connecting ...");
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + addressString);
        System.out.println("Connected!");
        System.out.println("Creating tables ...");
        long start = System.nanoTime();
        this.createTables(conn.createStatement(), recreateTables);
        long diff = System.nanoTime() - start;
        System.out.println("Created tables in " + diff / 1000000000.0 + "s!");
        conn.close();

        // Get caches
        System.out.println("Initialize caches ...");
        start = System.nanoTime();
        this.illCache = this.client.cache(IGNITE_PREFIX + "ILL");
        this.infoCache = this.client.cache(IGNITE_PREFIX + "INFO");
        this.treatCache = this.client.cache(IGNITE_PREFIX + "TREAT");
        if (clearTables)
            clearTables();
        diff = System.nanoTime() - start;
        System.out.println("Initialized all the caches in " + diff / 1000000000.0 + "s : " + client.cacheNames());

        // Populate
        try {
            this.putRandomData(p, d);
        } catch (SimilarityException e) {
            e.printStackTrace();
            System.out.println("Stopped population due to a similarity error! " + e.getMessage());
        }
        this.disconnectClient();
    }


// #################################### Private Methods ###########################################


    /**
     * Drop tables if they already exist, then, create tables via SQL CREATE statements.
     *
     * @param stmt           Statement to be used for dropping and creating tables
     * @param recreateTables If this is true, the tables will be dropped and recreated!
     * @throws SQLException Error when parsing or executing the SQL query
     */
    private void createTables(Statement stmt, boolean recreateTables) throws SQLException {
        // CREATE TABLE STATEMENTS
        if (recreateTables)
            stmt.executeUpdate("DROP TABLE IF EXISTS INFO");
        String create =
                "CREATE TABLE IF NOT EXISTS INFO (" +
                        "ID INT PRIMARY KEY," +
                        "Name VARCHAR," +
                        "Address VARCHAR," +
                        "Age INT) " +
                        "WITH \"template=infoTemplate,key_type=materializedfragments.FragIDKey," +
                        "value_type=materializedfragments.Info\"";
        stmt.executeUpdate(create);
        System.out.println("Created Table INFO!");

        if (recreateTables)
            stmt.executeUpdate("DROP TABLE IF EXISTS ILL");
        create =
                "CREATE TABLE IF NOT EXISTS ILL (" +
                        "ID INT, " +
                        "Disease VARCHAR, " +
                        "CUI VARCHAR, " +
                        "PRIMARY KEY (ID, Disease)) " +
                        "WITH \"template=illTemplate,key_type=materializedfragments.IllKey," +
                        "value_type=materializedfragments.Ill\"";
        stmt.executeUpdate(create);
        System.out.println("Created Table ILL!");

        if (recreateTables)
            stmt.executeUpdate("DROP TABLE IF EXISTS TREAT");
        create =
                "CREATE TABLE IF NOT EXISTS TREAT (" +
                        "ID INT, " +
                        "Prescription VARCHAR, " +
                        "Success BOOLEAN, " +
                        "PRIMARY KEY (ID, Prescription)) " +
                        "WITH \"template=treatTemplate,key_type=materializedfragments.FragIDKey," +
                        "value_type=materializedfragments.Treat\"";
        stmt.executeUpdate(create);
        System.out.println("Created Table TREAT!");

    }


    /**
     * Clear all the tables.
     */
    private void clearTables() {
        this.illCache.clear();
        this.infoCache.clear();
        this.treatCache.clear();
    }


    /**
     * Put random data.
     *
     * @param p p Persons
     * @param d d Diseases (and prescriptions)
     * @throws SimilarityException If an exception occurs while calculating similarity
     */
    private void putRandomData(long p, long d) throws SimilarityException {

        System.out.println("Populating caches ...");
        long start = System.nanoTime();

        // Get all terms
        ArrayList<String> terms = this.affinityFunction.getTerms();
        int numTerms = terms.size();

        // Generate all the persons before (without partition in key, this is set later)
        System.out.println("Generating " + p + " random persons ...");
        Info[] persons = IgniteUtils.generatePersons(p);
        System.out.println("Generated " + p + " random persons!");

        System.out.println("Generating " + d + " random disease entries in the db ...");
        for (int i = 0; i < d; i++) {

            // Some random disease for a random personID (bound by argument p)
            Random random = new Random(System.nanoTime());
            int indexTerms = random.nextInt(numTerms);
            String disease = terms.get(indexTerms);
            int personID = random.nextInt((int) p);
            String cui = affinityFunction.getCUI(disease);

            // Create InfoKey- and Ill-Objects
            IllKey key = new IllKey(personID, disease);
            Ill ill = new Ill(key, cui);

            // Info-object for the same partition (if not present yet with another disease of same cluster)
            Info person = persons[personID];
            int fragID = this.affinityFunction.identifyCluster(disease);
            person.getKey().setFragID(fragID);

            // Treat object with the same FragIDKey as the person (collocation)
            Treat treat = new Treat(person.getKey(), "Prescr.XY.01", null);

            this.treatCache.put(treat.getKey(), treat);
            this.infoCache.put(person.getKey(), person);
            this.illCache.put(key, ill);

            System.out.println("Finished " + i * 100.0 / d + "%");  // For progress information
        }

        long diff = System.nanoTime() - start;
        System.out.println("Finished population in " + diff / 1000000000.0 + "s!");
    }


    /**
     * Initialize the similarity cache by getting the cache instance (or creating it if not yet created),
     * loading the pairwise similarity data into the cache and adapting the {@link SimTableSimilarity}.
     */
    private void initSimCache() {
        final String CACHE_NAME = "SIM";
        System.out.println("Initializing cache '" + CACHE_NAME + "' ...");
        long start = System.nanoTime();
        this.simCache = client.getOrCreateCache(CACHE_NAME);
        this.simCache.loadCache(null);      // use SimilarityCacheStore to load all similarities
        long diff = System.nanoTime() - start;
        System.out.println("Initialized cache '" + CACHE_NAME + "' in " + diff / 1000000000.0 + "s!");
        SimilarityFunction<String> similarityFunction = affinityFunction.getSimilarityFunction();
        if (similarityFunction instanceof SimTableSimilarity)
            ((SimTableSimilarity) similarityFunction).setSimCache(this.simCache);   // Set simCache in sim function
    }


    /**
     * Initialize the clustering cache by getting the cache instance (or creating it if not yet created),
     * loading the pairwise clustering information (cluster id, head term) into the cache and adapting
     * the {@link SimClusteringTableSimilarity}.
     */
    private void initClusteringCache() {
        final String CACHE_NAME = "CLUSTERING";
        System.out.println("Initializing cache '" + CACHE_NAME + "' ...");
        long start = System.nanoTime();
        this.clusteringCache = client.getOrCreateCache(CACHE_NAME);
        this.clusteringCache.loadCache(null);
        long diff = System.nanoTime() - start;
        System.out.println("Initialized cache '" + CACHE_NAME + "' in " + diff / 1000000000.0 + "s!");
        SimilarityFunction<String> similarityFunction = affinityFunction.getSimilarityFunction();
        if (similarityFunction instanceof SimClusteringTableSimilarity)
            ((SimClusteringTableSimilarity) similarityFunction).setSimCache(this.simCache);   // Set simCache
    }


// #################################### Public Methods ############################################

    /**
     * Close an open connection to the cluster.
     */
    public void disconnectClient() {
        if (this.client != null)
            this.client.close();
    }

}
