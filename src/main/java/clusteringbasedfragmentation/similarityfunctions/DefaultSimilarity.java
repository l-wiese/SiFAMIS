package clusteringbasedfragmentation.similarityfunctions;

import clusteringbasedfragmentation.SimilarityException;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

/**
 * <p>
 * This class implements the simple default similarity calculation functionality with a predefined
 * small term set ({@link DefaultSimilarity#DFLT_TERMS}) and a {@link HashMap} for storing the
 * pairwise similarities.
 * </p>
 * <p>
 * Only used exemplary or for testing/debugging purposes
 * </p>
 */
public class DefaultSimilarity implements MeSHSimilarityFunction, Serializable {


    private static final long serialVersionUID = -5566736673200584701L;


    /**
     * Default term set
     */
    private static final String[] DFLT_TERMS = {"Asthma", "Cough", "Influenza", "Ulna Fracture", "Tibial Fracture"};

    /**
     * HashMap for the pairwise similarities of predefined MeSH terms (active domain here), stored with key
     * "term1+term2" and value as double
     */
    private HashMap<String, Double> similarities;

    /**
     * HashMap of all the terms (keys) occuring in the active domain of the relaxation attribute and their
     * Concept Unique Identifiers (CUIs)
     */
    private HashMap<String, String> terms;

    /**
     * Constructor for default similarity over the fixed term set {@link DefaultSimilarity#DFLT_TERMS}.
     */
    public DefaultSimilarity() {

        this.terms = new HashMap<>();
        for (String s : DFLT_TERMS)
            terms.put(s, "CUI DOES NOT MATTER HERE");

        // Init similarities HashMap with predefined active domain DFLT_TERMS
        this.similarities = new HashMap<>();
        this.similarities.put("Asthma+Cough", 0.2);                         //0.2<>Asthma<>Cough
        this.similarities.put("Asthma+Influenza", 0.2);                     //0.2<>Asthma<>Influenza
        this.similarities.put("Asthma+Tibial Fracture", 0.1429);            //0.1429<>Asthma<>Tibial Fracture
        this.similarities.put("Asthma+Ulna Fracture", 0.1429);              //0.1429<>Asthma<>Ulna Fracture
        this.similarities.put("Cough+Influenza", 0.2);                      //0.2<>Cough<>Influenza
        this.similarities.put("Cough+Tibial Fracture", 0.1429);             //0.1429<>Cough)<>Tibial Fracture
        this.similarities.put("Cough+Ulna Fracture", 0.1429);               //0.1429<>Cough<>Ulna Fracture
        this.similarities.put("Influenza+Tibial Fracture", 0.1429);         //0.1429<>Influenza<>Tibial Fracture
        this.similarities.put("Influenza+Ulna Fracture", 0.1429);           //0.1429<>Influenza<>Ulna Fracture
        this.similarities.put("Tibial Fracture+Ulna Fracture", 0.3333);     //.3333<>Tibial Fracture<>Ulna Fracture

    }


    /**
     * Calculate the similarity of two MeSH terms.
     *
     * @param term1 MeSH term
     * @param term2 MeSH term
     * @return Similarity
     */
    @Override
    public double similarity(String term1, String term2) throws SimilarityException {
        Double sim;
        if ((sim = similarities.get(term1 + "+" + term2)) != null)
            return sim;
        if ((sim = similarities.get(term2 + "+" + term1)) != null)
            return sim;
        throw new SimilarityException("No similarity found for '" + term1 + "' and '" + term2 + "'!");
    }

    /**
     * Get all terms and their CUIs. Note CUIs are not supported here.
     *
     * @return HashMap with keys=terms, values=CUIs
     */
    @Override
    public HashMap<String, String> getTermsWithCUIs() {
        return terms;
    }


    /**
     * Get a set of all terms.
     *
     * @return Term set
     */
    @Override
    public Set<String> getTerms() {
        return terms.keySet();
    }
}
