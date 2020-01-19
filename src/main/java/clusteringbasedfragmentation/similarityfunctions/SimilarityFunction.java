package clusteringbasedfragmentation.similarityfunctions;

import clusteringbasedfragmentation.SimilarityException;

/**
 * Basic interface for pairwise similarity functions.
 */
public interface SimilarityFunction<T> {

    /**
     * Calculate the similarity of two objects of type T.
     *
     * @param term1 Object 1
     * @param term2 Object 2
     * @return Similarity value
     */
    public double similarity(T term1, T term2) throws SimilarityException;


}
