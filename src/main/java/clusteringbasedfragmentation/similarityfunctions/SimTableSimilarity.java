package clusteringbasedfragmentation.similarityfunctions;

import clusteringbasedfragmentation.Similarity;
import clusteringbasedfragmentation.SimilarityException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import utils.IgniteUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class provides the implementation of the pairwise MeSH similarities as
 * database table (cache) in the Apache Ignite cluster. The similarities are stored
 * as key-value pairs where the key is the concatenation of the two terms and the
 * value is the similarity value  between these two MeSH terms.
 */
public class SimTableSimilarity implements MeSHSimilarityFunction, Serializable {

    private static final long serialVersionUID = 4565013967878209464L;

    /**
     * Name of the similarity cache
     */
    protected String simCacheName;

    /**
     * Set of terms
     */
    protected Set<String> terms;

    /**
     * Flag indicating whether this similarity function acts inside the cluster
     * (server-side) where the table is locally available or whether it wants to
     * connect to the cluster from outside (client-side) remotely.
     */
    protected boolean isLocal;

    /**
     * Similarity cache instance (name equals field {@link SimTableSimilarity#simCacheName})
     */
    protected transient IgniteCache<String, Similarity> simCache;

// ####################################### Constructors ######################################


    /**
     * Empty constructor
     */
    public SimTableSimilarity() {
        this("");
    }

    /**
     * Set up similarity table similarity function with a cache name
     *
     * @param simCacheName Name of the similarity cache
     */
    public SimTableSimilarity(String simCacheName) {
        this(simCacheName, new HashSet<>());
    }

    /**
     * Similarity function with SimTable and predefined term set.
     *
     * @param simCacheName Name of similarity cache/table
     * @param terms     Set of MeSH terms
     */
    public SimTableSimilarity(String simCacheName, HashSet<String> terms) {
        this(simCacheName, terms, false);
    }

    /**
     * SimTable similarity function with similarity cache name and isLocal flag.
     *
     * @param simCacheName Name of the similarity table
     * @param isLocal   If set to true, this similarity function acts tries to find the
     *                  similarity table locally (i.e. server-side). If set to false, it
     *                  will try to connect to the Ignite cluster remotely (client-side)
     *                  to get the similarity table.
     */
    public SimTableSimilarity(String simCacheName, boolean isLocal) {
        this(simCacheName, new HashSet<>(), isLocal);
    }

    /**
     * SimTable similarity function with similarity cache name, predefined MeSH
     * term set and isLocal flag.
     *
     * @param simCacheName Name of the similarity table
     * @param terms     Set of MeSH terms
     * @param isLocal   If set to true, this similarity function acts tries to find the
     *                  similarity table locally (i.e. server-side). If set to false, it
     *                  will try to connect to the Ignite cluster remotely (client-side)
     *                  to get the similarity table.
     */
    public SimTableSimilarity(String simCacheName, Set<String> terms, boolean isLocal) {
        this(simCacheName, terms, isLocal, null);
    }

    /**
     * SimTable similarity function with similarity cache name, predefined MeSH
     * term set, isLocal flag and a similarity cache instance.
     *
     * @param simCacheName Name of the similarity table
     * @param terms     Set of MeSH terms
     * @param isLocal   If set to true, this similarity function acts tries to find the
     *                  similarity table locally (i.e. server-side). If set to false, it
     *                  will try to connect to the Ignite cluster remotely (client-side)
     *                  to get the similarity table.
     * @param simCache  Similarity cache instance
     */
    public SimTableSimilarity(String simCacheName, Set<String> terms, boolean isLocal,
                              IgniteCache<String, Similarity> simCache) {

        this.simCacheName = simCacheName;
        this.terms = terms;
        this.isLocal = isLocal;
        this.simCache = simCache;
    }


// ##################################### Getter & Setter #####################################

    /**
     * Get the name of the similarity cache
     *
     * @return Cache name
     */
    public String getSimCacheName() {
        return simCacheName;
    }

    /**
     * Set the name of the similarity cache (note: does not change the name of the underlying
     * {@link IgniteCache} but only the field of this object
     *
     * @param simCacheName New cache name
     * @return {@code This} for chaining
     */
    public SimTableSimilarity setSimCacheName(String simCacheName) {
        this.simCacheName = simCacheName;
        return this;
    }

    /**
     * Set MeSH term set
     *
     * @param terms Term set
     * @return {@code This} for chaining
     */
    public SimTableSimilarity setTerms(Set<String> terms) {
        this.terms = terms;
        return this;
    }

    /**
     * Get isLocal flag.
     *
     * @return Returns true if the cache is obtained locally, false if is obtained remotely.
     */
    public boolean isLocal() {
        return isLocal;
    }

    /**
     * Set is isLocal flag.
     *
     * @param local isLocal flag
     * @return {@code This} for chaining
     */
    public SimTableSimilarity setLocal(boolean local) {
        isLocal = local;
        return this;
    }

    /**
     * Get the similarity cache instance.
     *
     * @return Similarity cache
     */
    public IgniteCache<String, Similarity> getSimCache() {
        return simCache;
    }

    /**
     * Set the similarity cache instance
     *
     * @param simCache Similarity cache
     * @return {@code This} for chaining
     */
    public SimTableSimilarity setSimCache(IgniteCache<String, Similarity> simCache) {
        this.simCache = simCache;
        return this;
    }


// ################################### Overwritten Methods ###################################

    /**
     * Calculate the similarity of two MeSH terms by querying the SimTable.
     *
     * @param term1 MeSH term
     * @param term2 MeSH term
     * @return Similarity value
     */
    @Override
    public double similarity(String term1, String term2) throws SimilarityException {

        // Check if simCache instance already available
        if (simCache == null & isLocal)
            simCache = Ignition.ignite().cache(this.simCacheName);
        else if (simCache == null & !isLocal)
            simCache = Ignition.getOrStart(IgniteUtils.createIgniteConfig(
                    Arrays.asList("141.5.107.8", "141.5.107.75", "141.5.107.76"), true)     // TODO addresses
            ).cache(simCacheName);

        // Combine the two terms to the key (lexicographically ordered, concatenated with a '+')
        String key;
        int compValue = term1.compareTo(term2);
        if (compValue == 0)
            return 1;
        else if (compValue < 0)
            key = term1 + "+" + term2;
        else
            key = term2 + "+" + term1;

        return simCache.get(key).getSimvalue();
    }

    /**
     * Get all terms and their CUIs. Not supported here!
     *
     * @return HashMap with keys=terms, values=CUIs
     */
    @Override
    public Map<String, String> getTermsWithCUIs() {
        // Not supported
        return null;
    }

    /**
     * Get a set of all terms.
     *
     * @return Term set
     */
    @Override
    public Set<String> getTerms() {
        return this.terms;
    }


// ######################################### Others ##########################################

    /**
     * Add term to term set
     *
     * @param term Term
     * @return {@code This} for chaining
     */
    public SimTableSimilarity addTerm(String term) {
        this.terms.add(term);
        return this;
    }


}
