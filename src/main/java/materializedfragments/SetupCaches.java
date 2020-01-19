package materializedfragments;

import clusteringbasedfragmentation.*;
import clusteringbasedfragmentation.similarityfunctions.SimClusteringTableSimilarity;
import clusteringbasedfragmentation.similarityfunctions.SimTableSimilarity;
import clusteringbasedfragmentation.similarityfunctions.SimilarityFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import utils.IgniteUtils;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 * Setup the caches (tables) for the materialized fragment approach.
 */
public class SetupCaches {

    /**
     * Affinity Function with clustering and partition mappings
     */
    private ClusteringAffinityFunction affinityFunction;


    /**
     * IP address(es) of Cluster Node(s).
     */
    private Collection<String> addresses;


    /**
     * Client node
     */
    private Ignite client;

    /**
     * Ignite configuration for the client
     */
    private IgniteConfiguration clientConfig;


    /**
     * Array list of all info fragment caches (index = fragment id).
     */
    private ArrayList<IgniteCache> infoCaches;


    /**
     * Array list of all ill fragment caches (index = fragment id).
     */
    private ArrayList<IgniteCache> illCaches;


    /**
     * Array list of all treat fragment caches (index = fragment id).
     */
    private ArrayList<IgniteCache> treatCaches;


    /**
     * Similarity cache (pairwise similarity values for disease terms).
     */
    private IgniteCache simCache;


    /**
     * Clustering cache (cluster id and head term)
     */
    private IgniteCache clusteringCache;


// ################################ Constructors ###############################################

    /**
     * Constructor.
     * CAUTION: Existing tables will be recreated (table data is lost)!
     *
     * @param affinityFunction Affinity Function for clustering-based fragmentation
     * @param addresses        Addresses of the ignite nodes for DiscoverySpi
     */
    public SetupCaches(ClusteringAffinityFunction affinityFunction, Collection<String> addresses)
            throws SQLException, ClassNotFoundException {
        this(affinityFunction, addresses, 0, 0, true, true, false, false);
    }


    /**
     * This constructor creates the tables via sql create table statements (Note: Number of terms used in affinity
     * function multiplied with param p must be significantly bigger than param size to avoid collisions)
     *
     * @param affinityFunction    Clustering-based affinity function
     * @param addresses           Addresses of the ignite nodes for DiscoverySpi
     * @param p                   Number of different INFO-Tuples generated (Note: still might be replicated due to derived fragmentation)
     * @param d                   Number of ILL-Tuples generated
     * @param recreateTables      If the tables are already created, then they will be recreated (DROP+CREATE) if this
     *                            flag is set to true. Otherwise, if the tables already exist, they will not be recreated
     * @param clearTables         If set to true, then all the tables content will be cleared. If this is set to false,
     *                            and recreateTables-flag is set to true, the tables will also be cleared.
     * @param initSimCache        If set to true, then the similarity cache will be created and initialized.
     * @param initClusteringCache If set to true, then the clustering cache will be created and initialized.
     */
    public SetupCaches(ClusteringAffinityFunction affinityFunction, Collection<String> addresses, long p, long d,
                       boolean recreateTables, boolean clearTables, boolean initSimCache, boolean initClusteringCache)
            throws ClassNotFoundException, SQLException {

        this.affinityFunction = affinityFunction;
        this.addresses = addresses;

        // Init configs & start client & init caches
        this.clientConfig = IgniteUtils.createIgniteConfig(addresses, true);
        this.client = Ignition.start(this.clientConfig);

        // Init sim cache & clustering cache if enabled
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

        // Create tables according to templates from xml config
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        System.out.println("Connecting ...");
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + String.join(",", addresses));
        System.out.println("Connected!");
        System.out.println("Creating tables ...");
        long start = System.nanoTime();
        createTables(conn, recreateTables);
        long diff = System.nanoTime() - start;
        System.out.println("Created tables in " + diff / 1000000000.0 + "s!");
        conn.close();

        // Init caches
        System.out.println("Initialize fragment caches ...");
        start = System.nanoTime();
        initCaches();
        if (clearTables)
            clearTables();
        diff = System.nanoTime() - start;
        System.out.println("Initialized fragment caches in " + diff / 1000000000.0 + "s!");

        // Fill the tables with data
        try {
            putRandomData(p, d);
        } catch (SimilarityException e) {
            e.printStackTrace();
            System.out.println("Stopped population due to an exception!");
        }
        this.disconnectClient();
    }


// ################################ Private Methods ############################################

    /**
     * Initialize the caches.
     */
    private void initCaches() {
        // As the caches are created with sql DDL statements their names are SQL_SCHEMANAME_TABLENAME
        // Default schemaname is PUBLIC
        String cachePrefix = "SQL_PUBLIC_";
        this.infoCaches = new ArrayList<>();
        this.illCaches = new ArrayList<>();
        this.treatCaches = new ArrayList<>();
        for (int id = 0; id < this.affinityFunction.getClusters().size(); id++) {
            this.infoCaches.add(id, this.client.cache(cachePrefix + "INFO_" + id));
            this.illCaches.add(id, this.client.cache(cachePrefix + "ILL_" + id));
            this.treatCaches.add(id, this.client.cache(cachePrefix + "TREAT_" + id));
        }
        System.out.println("Initialized " + infoCaches.size() + " Info Fragments");
        System.out.println("Initialized " + illCaches.size() + " Ill Fragments");
        System.out.println("Initialized " + treatCaches.size() + " Treat Fragments");
    }


    /**
     * (Re)create the tables via CREATE TABLE Sql statements
     *
     * @param conn           Connection
     * @param recreateTables If this is true, the tables will be dropped and recreated!
     * @throws SQLException Thrown if an exception occurs when executing the SQL "CREATE TABLE" statements
     */
    private void createTables(Connection conn, boolean recreateTables) throws SQLException {

        Statement stmt = conn.createStatement();
        for (int i = 0; i < this.affinityFunction.getClusters().size(); i++) {

            // INFO
            if (recreateTables)
                stmt.executeUpdate("DROP TABLE IF EXISTS INFO_" + i);

            String create =
                    "CREATE TABLE  IF NOT EXISTS INFO_" + i + " (" +
                            "ID INT PRIMARY KEY," +
                            "NAME VARCHAR," +
                            "ADDRESS VARCHAR," +
                            "AGE INT) " +
                            "WITH \"template=infoTemplate,key_type=materializedfragments.FragIDKey," +
                            "value_type=materializedfragments.Info\"";
            stmt.executeUpdate(create);

            // ILL
            if (recreateTables)
                stmt.executeUpdate("DROP TABLE IF EXISTS ILL_" + i);

            create =
                    "CREATE TABLE IF NOT EXISTS ILL_" + i + " (" +
                            "ID INT, " +
                            "DISEASE VARCHAR, " +
                            "CUI VARCHAR, " +
                            "PRIMARY KEY (ID, DISEASE)) " +
                            "WITH \"template=illFragmentTemplate,key_type=materializedfragments.IllKey," +
                            "value_type=materializedfragments.Ill\"";
            stmt.executeUpdate(create);


            // TREAT
            if (recreateTables)
                stmt.executeUpdate("DROP TABLE IF EXISTS TREAT_" + i);

            create =
                    "CREATE TABLE IF NOT EXISTS TREAT_" + i + " (" +
                            "ID INT, " +
                            "PRESCRIPTION VARCHAR, " +
                            "SUCCESS BOOLEAN, " +
                            "PRIMARY KEY (ID, PRESCRIPTION)) " +
                            "WITH \"template=treatTemplate,key_type=materializedfragments.FragIDKey," +
                            "value_type=materializedfragments.Treat\"";
            stmt.executeUpdate(create);
        }

        stmt.close();
    }


    /**
     * Clear all the tables.
     */
    private void clearTables() {

        for (IgniteCache cache : this.illCaches) {
            cache.clear();
        }
        for (IgniteCache cache : this.infoCaches) {
            cache.clear();
        }
        for (IgniteCache cache : this.treatCaches) {
            cache.clear();
        }

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

// ################################ Public Methods ############################################

    /**
     * Close the connection to the client
     */
    public void disconnectClient() {
        this.client.close();
    }

    /**
     * Randomly generates persons and puts data about them
     *
     * @param p Maximum number of different persons that are generated (might be less persons than this value but
     *          persons might be stored redundantly due to derived fragmentation)
     * @param d Defines how many Ill tuples are generated for maximal p persons and stored
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


        System.out.println("Filling ...");
        for (int i = 0; i < d; i++) {

            // Some random disease for a random personID (bound by argument p)
            Random random = new Random(System.nanoTime());
            String disease = terms.get(random.nextInt(numTerms));
            int fragID = this.affinityFunction.identifyCluster(disease);
            int personID = random.nextInt((int) p);
            String cui = this.affinityFunction.getCUI(disease);

            // Create Key- and Value-Objects & put ill-tuple to cache ill_id according to clustering
            IllKey key = new IllKey(personID, disease);
            Ill ill = new Ill(key, cui);
            this.illCaches.get(fragID).put(key, ill);

            // Put person
            Info person = persons[personID];
            FragIDKey fragKey = new FragIDKey(fragID, personID);
            person.setKey(fragKey);
            this.infoCaches.get(fragID).put(person.getKey(), person);

            // Put treat
            Treat treat = new Treat(fragKey, "Prescr.XY.01", null);
            this.treatCaches.get(fragID).put(treat.getKey(), treat);

            System.out.println("Filled " + i * 100.0 / d + "%");
        }

        long diff = System.nanoTime() - start;
        System.out.println("Finished population in " + diff / 1000000000.0 + "s!");
    }


    /**
     * Put random data with SQL INSERTs.
     *
     * @param conn Connection
     * @param p    p Persons
     * @param d    d Diseases
     * @throws SQLException        If an exception with the DB occurs
     * @throws SimilarityException If an exception occurs while calculating similarity
     */
    private void putRandomDataSQL(Connection conn, int p, int d) throws SQLException, SimilarityException {
        // Get all terms
        ArrayList<String> terms = this.affinityFunction.getTerms();
        int numTerms = terms.size();

        // Generate all the persons before (without partition in key, this is set later)
        System.out.println("Generating " + p + " random persons ...");
        Info[] persons = IgniteUtils.generatePersons(p);
        System.out.println("Generated " + p + " random persons!");

        // Prepared statements (because ' in names and addresses need to be escaped ...
        ArrayList<PreparedStatement> insertIll = new ArrayList<>();
        ArrayList<PreparedStatement> insertInfo = new ArrayList<>();
        for (int i = 0; i < this.affinityFunction.getClusters().size(); i++) {
            String insert = "INSERT INTO ILL_" + i + " (ID, DISEASE, CUI) VALUES (?, ?, ?)";
            insertIll.add(i, conn.prepareStatement(insert));

            insert = "INSERT INTO INFO_" + i + " (ID, NAME, ADDRESS, AGE) VALUES (?, ?, ?, ?)";
            insertInfo.add(i, conn.prepareStatement(insert));
        }

        System.out.println("Filling ...");
        for (int i = 0; i < d; i++) {

            // Some random disease for a random personID (bound by argument p)
            Random random = new Random(System.nanoTime());
            String disease = terms.get(random.nextInt(numTerms));
            int fragID = this.affinityFunction.identifyCluster(disease);
            int personID = random.nextInt(p);
            String cui = this.affinityFunction.getCUI(disease);

            // Insert in ill fragment
            PreparedStatement prep = insertIll.get(fragID);
            prep.setInt(1, personID);
            prep.setString(2, disease);
            prep.setString(3, cui);
            try {
                prep.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            // Insert in info fragment
            prep = insertInfo.get(fragID);
            Info person = persons[personID];
            prep.setInt(1, personID);
            prep.setString(2, person.getName());
            prep.setString(3, person.getAddress());
            prep.setInt(4, person.getAge());
            try {
                prep.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
                i--;
            }


            System.out.println("Filled " + (i * 100.0 / d) + "%");
        }
    }

// ################################## MAIN ######################################################

    /**
     * Test unit
     *
     * @param args List of cluster IP addresses, e.g. 123.234.102:1234 127.0.0.1:47500..47600
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException, SimilarityException {

        // Affinity function, calculates clustering in constructor
        String separ = File.separator;
        String termsFile = "csv" + separ + "terms10.txt";
        String simFile = "csv" + separ + "result10.csv";
        ClusteringAffinityFunction affinityFunction = new ClusteringAffinityFunction(0.15, termsFile, simFile);

        // Setup (Configs, create Caches)
        SetupCaches setup = new SetupCaches(affinityFunction, Arrays.asList(args),
                50, 100, false, true, false, false);
        setup.disconnectClient();
    }

}
