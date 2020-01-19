package clusteringbasedfragmentation;

import clusteringbasedfragmentation.similarityfunctions.SimTableSimilarity;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.cache.configuration.Factory;
import java.io.Serializable;

/**
 * Factory for {@link SimilarityCacheStore}.
 * Use this factory to pass {@link SimilarityCacheStore} to {@link CacheConfiguration}.
 */
public class SimilarityCacheStoreFactory implements Factory<SimilarityCacheStore>, Serializable {

    private static final long serialVersionUID = 6120575504232655457L;

    /**
     * Path to file containing pairwise pathlengths of MeSH terms
     */
    private String pathLengthCSV;

    /**
     * Similarity function that stores the pairwise similarities in an Ignite cache
     */
    private SimTableSimilarity simTableSimilarity;

    /**
     * Empty constructor
     */
    public SimilarityCacheStoreFactory() {
        pathLengthCSV = null;
        simTableSimilarity = null;
    }

    /**
     * Get the path to the pathlength csv file
     *
     * @return Path to file
     */
    public String getPathLengthCSV() {
        return pathLengthCSV;
    }

    /**
     * Set path to file containing pairwise pathlengths
     *
     * @param pathLengthCSV Path to file
     * @return {@code This} for chaining
     */
    public SimilarityCacheStoreFactory setPathLengthCSV(String pathLengthCSV) {
        this.pathLengthCSV = pathLengthCSV;
        return this;
    }

    /**
     * Get the SIM table similarity function
     *
     * @return Sim table similarity function
     */
    public SimTableSimilarity getSimTableSimilarity() {
        return simTableSimilarity;
    }

    /**
     * Set the SIM table similarity function.
     *
     * @param simTableSimilarity SIM table similarity function
     * @return {@code This} for chaining
     */
    public SimilarityCacheStoreFactory setSimTableSimilarity(SimTableSimilarity simTableSimilarity) {
        this.simTableSimilarity = simTableSimilarity;
        return this;
    }

    /**
     * Constructs and returns a fully configured instance of {@link SimilarityCacheStore}
     * as copy from {@code this} instance.
     *
     * @return SimilarityCacheStore instance
     */
    @Override
    public SimilarityCacheStore create() {
        return new SimilarityCacheStore(this.pathLengthCSV, this.simTableSimilarity);
    }
}
