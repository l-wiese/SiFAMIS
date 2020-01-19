package clusteringbasedfragmentation;

import clusteringbasedfragmentation.similarityfunctions.SimTableSimilarity;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheLoadOnlyStoreAdapter;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import javax.cache.integration.CacheLoaderException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * This class implements the functionality to load pairwise similarities of MeSH terms into the Ignite
 * cache from a pathlength csv file using the {@link CacheLoadOnlyStoreAdapter}.
 */
public class SimilarityCacheStore extends CacheLoadOnlyStoreAdapter<String, Similarity, String> implements Serializable {

    private static final long serialVersionUID = 2196961923711037569L;

    /**
     * Path to file containing pairwise pathlengths of MeSH terms
     */
    private String pathLengthCSV;

    /**
     * Similarity function that stores the pairwise similarities in an Ignite cache
     */
    private SimTableSimilarity simTableSimilarity;

// ####################################### Constructor ##############################################

    /**
     * Constructor for this cache adapter.
     *
     * @param pathLengthCSV      Path to file containing pairwise pathlengths of MeSH terms
     * @param simTableSimilarity Similarity function that stores the pairwise similarities in an Ignite cache
     */
    public SimilarityCacheStore(String pathLengthCSV, SimTableSimilarity simTableSimilarity) {
        this.pathLengthCSV = pathLengthCSV;
        this.simTableSimilarity = simTableSimilarity;
    }

// #################################### Getter & Setter ##########################################

    /**
     * Get the path to the pathlength csv file
     *
     * @return Path to file
     */
    public String getPathLengthCSV() {
        return pathLengthCSV;
    }


    /**
     * Get the SIM table similarity function
     *
     * @return Sim table similarity function
     */
    public SimTableSimilarity getSimTableSimilarity() {
        return simTableSimilarity;
    }


    // ################################### Overwritten Methods #########################################

    /**
     * Returns iterator of input records.
     * <p>
     * Note that returned iterator doesn't have to be thread-safe. Thus it could
     * operate on raw streams, DB connections, etc. without additional synchronization.
     * <p>
     * Load the csv file line-wise and return an iterator.
     *
     * @param args Arguments passes into {@link IgniteCache#loadCache(IgniteBiPredicate, Object...)} method.
     * @return Iterator over input records.
     * @throws CacheLoaderException If iterator can't be created with the given arguments.
     */
    @Override
    protected Iterator<String> inputIterator(@Nullable Object... args) throws CacheLoaderException {
        // Load the csv file line-wise and return an iterator
        try {
            return Files.lines(Paths.get(pathLengthCSV)).iterator();
        } catch (IOException e) {
            throw new CacheLoaderException(e);
        }
    }

    /**
     * This method should transform raw data records into valid key-value pairs
     * to be stored into cache.
     * <p>
     * If {@code null} is returned then this record will be just skipped.
     * <p>
     * Transform the read lines from the csv to similarities (term1+term2, double).
     * <p>
     * As a side effect, this also initializes the term set of the corresponding similarity function
     *
     * @param rec  A raw data record.
     * @param args Arguments passed into {@link IgniteCache#loadCache(IgniteBiPredicate, Object...)} method.
     * @return Cache entry to be saved in cache or {@code null} if no entry could be produced from this record.
     */
    @Nullable
    @Override
    protected IgniteBiTuple<String, Similarity> parse(String rec, @Nullable Object... args) {
        String[] parts = rec.split("\\|");
        String term1 = parts[0];
        String term2 = parts[1];

        // Add terms to term set of SimTableSimilarity
        simTableSimilarity.addTerm(term1);
        simTableSimilarity.addTerm(term2);

        // Ensure lexicographical order
        int comp = term1.compareTo(term2);
        if (comp < 0)
            return new IgniteBiTuple<>(term1 + "+" + term2,
                    new Similarity(term1, term2, 1.0 / Double.valueOf(parts[2])));
        else if (comp > 0)
            return new IgniteBiTuple<>(term2 + "+" + term1,
                    new Similarity(term1, term2, 1.0 / Double.valueOf(parts[2])));
        else    // This should normally not occur
            return new IgniteBiTuple<>(term1 + "+" + term1, new Similarity(term1, term1, 1.0));
    }


}
