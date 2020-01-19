package clusteringbasedfragmentation.similarityfunctions;

import java.util.Map;
import java.util.Set;

/**
 * Interface for {@link SimilarityFunction <String>} that deal with MeSH terms
 */
public interface MeSHSimilarityFunction extends SimilarityFunction<String> {

    /**
     * Get all terms and their CUIs.
     *
     * @return HashMap with keys=terms, values=CUIs
     */
    Map<String, String> getTermsWithCUIs();

    /**
     * Get a set of all terms.
     *
     * @return Term set
     */
    Set<String> getTerms();

}
