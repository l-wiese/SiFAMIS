package clusteringbasedfragmentation;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheLoadOnlyStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This class implements the functionality to load the cluster id's and the corresponding cluster heads into
 * a cache from file containing the clustering serialization.
 * <p>
 * For details see {@link CacheLoadOnlyStoreAdapter}. Note that {@link CacheStoreAdapter#delete(Object)} and
 * {@link CacheStoreAdapter#write(Cache.Entry)} are not supported as the underlying clustering should not
 * be changed.
 */
public class ClusteringCacheStore extends CacheLoadOnlyStoreAdapter<Integer, String, IgniteBiTuple<Integer, String>>
        implements Serializable {

    private static final long serialVersionUID = 3505038760690211520L;

    /**
     * Path to file containing the clustering
     */
    private String clusteringFile;

// ################################### Constructor #########################################

    /**
     * Constructor that loads the clustering from the file
     * @param clusteringFile Path to file containing the serialized clustering
     */
    public ClusteringCacheStore(String clusteringFile) throws IOException, ClassNotFoundException {
        this.clusteringFile = clusteringFile;
    }


// ################################### Getter & Setter #########################################

    public String getClusteringFile() {
        return clusteringFile;
    }

// ################################### Overwritten Methods #########################################


    /**
     * Returns iterator of input records.
     * <p>
     * Note that returned iterator doesn't have to be thread-safe. Thus it could
     * operate on raw streams, DB connections, etc. without additional synchronization.
     *
     * @param args Arguments passes into {@link IgniteCache#loadCache(IgniteBiPredicate, Object...)} method.
     * @return Iterator over input records.
     * @throws CacheLoaderException If iterator can't be created with the given arguments.
     */
    @Override
    protected Iterator<IgniteBiTuple<Integer, String>> inputIterator(@Nullable Object... args) throws CacheLoaderException {
        try {
            Clustering clustering = Clustering.deserializeFromFile(clusteringFile);
            ArrayList<IgniteBiTuple<Integer, String>> list = new ArrayList<>();
            for (int i = 0; i < clustering.size(); i++) {
                list.add(new IgniteBiTuple<>(i, clustering.getHead(i)));
            }
            return list.iterator();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error while accessing clustering in file " + clusteringFile);
            e.printStackTrace();
        }
        return null;
    }

    /**
     * This method should transform raw data records into valid key-value pairs
     * to be stored into cache.
     * <p>
     * If {@code null} is returned then this record will be just skipped.
     *
     * @param rec  A raw data record.
     * @param args Arguments passed into {@link IgniteCache#loadCache(IgniteBiPredicate, Object...)} method.
     * @return Cache entry to be saved in cache or {@code null} if no entry could be produced from this record.
     */
    @Nullable
    @Override
    protected IgniteBiTuple<Integer, String> parse(IgniteBiTuple<Integer, String> rec, @Nullable Object... args) {
        return rec;
    }

}
