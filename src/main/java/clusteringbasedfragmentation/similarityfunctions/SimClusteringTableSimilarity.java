package clusteringbasedfragmentation.similarityfunctions;

import clusteringbasedfragmentation.Similarity;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import utils.IgniteUtils;
import utils.SQLQueryUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This class implements the similarity function for the Ignite server mode with a similarity
 * and a clustering cache that are used to obtain the pairwise similarities between two MeSH terms
 * and to identify the most similar cluster from the underlying clustering-based fragmentation to
 * a given MeSH term. For this, an SQL TOP 1 query is evaluated by the Ignite cluster based on the
 * similarity and clustering caches.
 */
public class SimClusteringTableSimilarity extends SimTableSimilarity {

    private static final long serialVersionUID = 2729883110862056075L;

    /**
     * Name of the clustering cache. Must coincide with the name defined in the XML-configuration
     */
    private String clusteringCacheName;

    /**
     * Clustering cache instance.
     */
    private transient IgniteCache<Integer, String> clusteringCache;

    /**
     * <p>
     * Flag, to determine whether the {@link this#identifyCluster(String)} method may be used
     * for cluster identification or not. </p>
     * <p>
     * See {@link clusteringbasedfragmentation.ClusteringAffinityFunction#identifyCluster(String)}
     * for details.</p>
     */
    private boolean identifyClusterEnabled = false;

// ###################################### Constructors #############################################

    /**
     * Empty constructor
     */
    public SimClusteringTableSimilarity() {
        super();
        this.clusteringCacheName = "";
    }

    /**
     * Empty constructor
     */
    public SimClusteringTableSimilarity(String clusteringCacheName) {
        super();
        this.clusteringCacheName = clusteringCacheName;
    }

    /**
     * Set up similarity and clustering cache similarity function with the cache names
     *
     * @param simCacheName        Name of the similarity cache
     * @param clusteringCacheName Name of the clustering cache
     */
    public SimClusteringTableSimilarity(String simCacheName, String clusteringCacheName) {
        super(simCacheName);
        this.clusteringCacheName = clusteringCacheName;
    }

    /**
     * Similarity function with similarity and clustering caches and predefined term set.
     *
     * @param simCacheName        Name of similarity cache
     * @param terms               Set of MeSH terms
     * @param clusteringCacheName Name of the clustering cache
     */
    public SimClusteringTableSimilarity(String simCacheName, HashSet<String> terms, String clusteringCacheName) {
        super(simCacheName, terms);
        this.clusteringCacheName = clusteringCacheName;
    }

    /**
     * SimTable similarity function with similarity and clustering cache names and isLocal flag.
     *
     * @param simCacheName        Name of the similarity table
     * @param isLocal             If set to true, this similarity function acts tries to find the
     *                            similarity table locally (i.e. server-side). If set to false, it
     *                            will try to connect to the Ignite cluster remotely (client-side)
     * @param clusteringCacheName Name of the clustering cache
     */
    public SimClusteringTableSimilarity(String simCacheName, boolean isLocal, String clusteringCacheName) {
        super(simCacheName, isLocal);
        this.clusteringCacheName = clusteringCacheName;
    }

    /**
     * SimTable similarity function with similarity and clustering cache names, predefined MeSH
     * term set and isLocal flag.
     *
     * @param simCacheName        Name of the similarity table
     * @param terms               Set of MeSH terms
     * @param isLocal             If set to true, this similarity function acts tries to find the
     *                            similarity table locally (i.e. server-side). If set to false, it
     *                            will try to connect to the Ignite cluster remotely (client-side)
     * @param clusteringCacheName Name of the clustering cache
     */
    public SimClusteringTableSimilarity(String simCacheName, Set<String> terms, boolean isLocal, String clusteringCacheName) {
        super(simCacheName, terms, isLocal);
        this.clusteringCacheName = clusteringCacheName;
    }

    /**
     * SimTable similarity function with similarity and clustering cache names, predefined MeSH
     * term set, isLocal flag and a similarity cache instance.
     *
     * @param simCacheName        Name of the similarity table
     * @param terms               Set of MeSH terms
     * @param isLocal             If set to true, this similarity function acts tries to find the
     *                            similarity table locally (i.e. server-side). If set to false, it
     *                            will try to connect to the Ignite cluster remotely (client-side)
     *                            to get the similarity table.
     * @param simCache            Similarity cache instance
     * @param clusteringCacheName Name of the clustering cache
     */
    public SimClusteringTableSimilarity(String simCacheName, Set<String> terms, boolean isLocal, IgniteCache<String, Similarity> simCache, String clusteringCacheName) {
        super(simCacheName, terms, isLocal, simCache);
        this.clusteringCacheName = clusteringCacheName;
    }

    /**
     * SimTable similarity function with similarity and clustering cache names, predefined MeSH
     * term set, isLocal flag and a similarity cache instance.
     *
     * @param simCacheName        Name of the similarity table
     * @param terms               Set of MeSH terms
     * @param isLocal             If set to true, this similarity function acts tries to find the
     *                            similarity table locally (i.e. server-side). If set to false, it
     *                            will try to connect to the Ignite cluster remotely (client-side)
     *                            to get the similarity table.
     * @param simCache            Similarity cache instance
     * @param clusteringCacheName Name of the clustering cache
     * @param clusteringCache     Clustering cache instance
     */
    public SimClusteringTableSimilarity(String simCacheName, Set<String> terms, boolean isLocal, IgniteCache<String, Similarity> simCache, String clusteringCacheName, IgniteCache<Integer, String> clusteringCache) {
        super(simCacheName, terms, isLocal, simCache);
        this.clusteringCacheName = clusteringCacheName;
        this.clusteringCache = clusteringCache;
    }

    /**
     * SimTable similarity function with clustering cache name, clustering cache and identify cluster flag
     *
     * @param clusteringCacheName    Name of the clustering cache
     * @param clusteringCache        Clustering cache instance
     * @param identifyClusterEnabled Specifies if usage of {@link this#identifyCluster(String)} )} is enabled
     */
    public SimClusteringTableSimilarity(String clusteringCacheName, IgniteCache<Integer, String> clusteringCache, boolean identifyClusterEnabled) {
        this.clusteringCacheName = clusteringCacheName;
        this.clusteringCache = clusteringCache;
        this.identifyClusterEnabled = identifyClusterEnabled;
    }

    /**
     * SimTable similarity function with sim and clustering cache names, clustering cache and identify cluster flag
     *
     * @param simCacheName           Name of the similarity cache
     * @param clusteringCacheName    Name of the clustering cache
     * @param clusteringCache        Clustering cache instance
     * @param identifyClusterEnabled Specifies if usage of {@link this#identifyCluster(String)} )} is enabled
     */
    public SimClusteringTableSimilarity(String simCacheName, String clusteringCacheName, IgniteCache<Integer, String> clusteringCache, boolean identifyClusterEnabled) {
        super(simCacheName);
        this.clusteringCacheName = clusteringCacheName;
        this.clusteringCache = clusteringCache;
        this.identifyClusterEnabled = identifyClusterEnabled;
    }

    /**
     * SimTable similarity function with sim and clustering cache names, predefined MeSH
     * term set, clustering cache and identify cluster flag
     *
     * @param simCacheName           Name of the similarity cache
     * @param terms                  Predefined MeSH term set
     * @param clusteringCacheName    Name of the clustering cache
     * @param clusteringCache        Clustering cache instance
     * @param identifyClusterEnabled Specifies if usage of {@link this#identifyCluster(String)} )} is enabled
     */
    public SimClusteringTableSimilarity(String simCacheName, HashSet<String> terms, String clusteringCacheName, IgniteCache<Integer, String> clusteringCache, boolean identifyClusterEnabled) {
        super(simCacheName, terms);
        this.clusteringCacheName = clusteringCacheName;
        this.clusteringCache = clusteringCache;
        this.identifyClusterEnabled = identifyClusterEnabled;
    }

    /**
     * SimTable similarity function with similarity and clustering cache names, predefined MeSH
     * term set, isLocal flag and a similarity cache instance. Also sets flag for identifyCluster
     * method usage.
     *
     * @param simCacheName           Name of the similarity table
     * @param isLocal                If set to true, this similarity function acts tries to find the
     *                               similarity table locally (i.e. server-side). If set to false, it
     *                               will try to connect to the Ignite cluster remotely (client-side)
     *                               to get the similarity table.
     * @param clusteringCacheName    Name of the clustering cache
     * @param clusteringCache        Clustering cache instance
     * @param identifyClusterEnabled Specifies if usage of {@link this#identifyCluster(String)} )} is enabled
     */
    public SimClusteringTableSimilarity(String simCacheName, boolean isLocal, String clusteringCacheName, IgniteCache<Integer, String> clusteringCache, boolean identifyClusterEnabled) {
        super(simCacheName, isLocal);
        this.clusteringCacheName = clusteringCacheName;
        this.clusteringCache = clusteringCache;
        this.identifyClusterEnabled = identifyClusterEnabled;
    }

    /**
     * SimTable similarity function with similarity and clustering cache names, predefined MeSH
     * term set, isLocal flag and a similarity cache instance. Also sets flag for identifyCluster
     * method usage.
     *
     * @param simCacheName           Name of the similarity table
     * @param isLocal                If set to true, this similarity function acts tries to find the
     *                               similarity table locally (i.e. server-side). If set to false, it
     *                               will try to connect to the Ignite cluster remotely (client-side)
     *                               to get the similarity table.
     * @param terms                  Predefined MeSH term set
     * @param clusteringCacheName    Name of the clustering cache
     * @param clusteringCache        Clustering cache instance
     * @param identifyClusterEnabled Specifies if usage of {@link this#identifyCluster(String)} )} is enabled
     */
    public SimClusteringTableSimilarity(String simCacheName, Set<String> terms, boolean isLocal, String clusteringCacheName, IgniteCache<Integer, String> clusteringCache, boolean identifyClusterEnabled) {
        super(simCacheName, terms, isLocal);
        this.clusteringCacheName = clusteringCacheName;
        this.clusteringCache = clusteringCache;
        this.identifyClusterEnabled = identifyClusterEnabled;
    }

    /**
     * SimTable similarity function with similarity and clustering cache names, predefined MeSH
     * term set, isLocal flag and a similarity cache instance. Also sets flag for identifyCluster
     * method usage.
     *
     * @param simCacheName           Name of the similarity table
     * @param isLocal                If set to true, this similarity function acts tries to find the
     *                               similarity table locally (i.e. server-side). If set to false, it
     *                               will try to connect to the Ignite cluster remotely (client-side)
     *                               to get the similarity table.
     * @param terms                  Predefined MeSH term set
     * @param simCache               Similarity cache instance
     * @param clusteringCacheName    Name of the clustering cache
     * @param clusteringCache        Clustering cache instance
     * @param identifyClusterEnabled Specifies if usage of {@link this#identifyCluster(String)} )} is enabled
     */
    public SimClusteringTableSimilarity(String simCacheName, Set<String> terms, boolean isLocal, IgniteCache<String, Similarity> simCache, String clusteringCacheName, IgniteCache<Integer, String> clusteringCache, boolean identifyClusterEnabled) {
        super(simCacheName, terms, isLocal, simCache);
        this.clusteringCacheName = clusteringCacheName;
        this.clusteringCache = clusteringCache;
        this.identifyClusterEnabled = identifyClusterEnabled;
    }

    /**
     * SimTable similarity function with similarity and clustering cache name, isLocal flag and
     * identifyClusterEnabled flag (this constructor is used by the XML-configurations of the
     * Ignite servers).
     *
     * @param simCacheName           Name of the similarity table
     * @param isLocal                If set to true, this similarity function acts tries to find the
     *                               similarity table locally (i.e. server-side). If set to false, it
     *                               will try to connect to the Ignite cluster remotely (client-side)
     * @param clusteringCacheName    Name of the clustering cache
     * @param identifyClusterEnabled Specifies if usage of {@link this#identifyCluster(String)} )} is enabled
     */
    public SimClusteringTableSimilarity(String simCacheName, boolean isLocal, String clusteringCacheName, boolean identifyClusterEnabled) {
        super(simCacheName, isLocal);
        this.clusteringCacheName = clusteringCacheName;
        this.identifyClusterEnabled = identifyClusterEnabled;
    }


    // #################################### Getter & Setter ############################################

    /**
     * Get clustering cache name.
     * @return Cache name
     */
    public String getClusteringCacheName() {
        return clusteringCacheName;
    }

    /**
     * Set clustering cache name.
     * Note: If the cache instance has already been created/gotten, invoking this will
     * release the old instance and force to recreate the instance when it is needed again!
     * @param clusteringCacheName
     */
    public void setClusteringCacheName(String clusteringCacheName) {
        this.clusteringCacheName = clusteringCacheName;
        if (this.clusteringCache != null) {
            this.clusteringCache = null;
        }
    }

    /**
     * Get identifyClusterEnabled flag
     * @return True if {@link this#identifyCluster(String)} is enabled, false otherwise
     */
    public boolean isIdentifyClusterEnabled() {
        return identifyClusterEnabled;
    }

    /**
     * Set identifyClusterEnabled flag
     * @param identifyClusterEnabled Flag value
     */
    public void setIdentifyClusterEnabled(boolean identifyClusterEnabled) {
        this.identifyClusterEnabled = identifyClusterEnabled;
    }

    // ################################## Similarity Methods ##########################################


    /**
     * Identify the most similar cluster to a given MeSH term by an SQL TOP 1 query.
     *
     * @param term MeSH Term
     * @return ID of the most similar cluster
     * @throws NoSuchMethodException Thrown if this method is not enabled.
     */
    public int identifyCluster(String term) throws NoSuchMethodException {

        // check if allowed, deny if disallowed (should not occur if implemented correctly)
        if (!identifyClusterEnabled)
            throw new NoSuchMethodException("This method is not enabled!\n" +
                    "If you are using a '...clustCache.xml' server configuration, then --initClu flag must be set.\n" +
                    "If this is not the case, then the --initClu must not be set.");

        // Check if clustering cache already available
        if (clusteringCache == null & isLocal) {
            clusteringCache = Ignition.ignite().cache(clusteringCacheName);
        } else if (clusteringCache == null & !isLocal) {
            clusteringCache = Ignition.getOrStart(IgniteUtils.createIgniteConfig(
                    Arrays.asList("141.5.107.8", "141.5.107.75", "141.5.107.76"), true)     // TODO
            ).cache(clusteringCacheName);
        }

        // SQL query for the TOP 1 cluster (most similar one to the given term)
        String escaped = SQLQueryUtils.escape(term);
        String sql =
                "SELECT TOP 1 id\n" +
                        "FROM (\n" +
                        "    SELECT c.id AS id, s.simvalue AS sim\n" +
                        "    FROM CLUSTERING c JOIN SIM s on c.head = s.term1\n" +
                        "    WHERE s.term2 = '" + escaped + "'\n" +
                        "    UNION ALL\n" +
                        "    SELECT c.id AS id, s.simvalue AS sim\n" +
                        "    FROM CLUSTERING c JOIN SIM s on c.head = s.term2\n" +
                        "    WHERE s.term1 = '" + escaped + "'\n" +
                        ")\n" +
                        "ORDER BY sim DESC";

        SqlFieldsQuery fieldsQuery = new SqlFieldsQuery(sql);
        fieldsQuery.setReplicatedOnly(true);

        // only a single value is returned as query result
        return (Integer) clusteringCache.query(fieldsQuery).iterator().next().get(0);
    }


}
