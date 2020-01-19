package clusteringbasedfragmentation;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * This class represents a similarity object between two MeSH terms.
 */
public class Similarity {

    @QuerySqlField
    /**
     * A MeSH term
     */
    private String term1;

    @QuerySqlField
    /**
     * A MeSH term
     */
    private String term2;

    @QuerySqlField
    /**
     * Similarity of the two MeSH terms {@link Similarity#term1} and {@link Similarity#term2}
     */
    private double simvalue;

    /**
     * Construct a similarity object for two given MeSH terms and a similarity value.
     *
     * @param term1    MeSH term
     * @param term2    MeSH term
     * @param simvalue Similarity value of both terms
     */
    public Similarity(String term1, String term2, double simvalue) {

        if (term1.compareTo(term2) < 0) {
            this.term1 = term1;
            this.term2 = term2;
        } else {
            this.term1 = term2;
            this.term2 = term1;
        }
        this.simvalue = simvalue;
    }

    /**
     * Get the first term
     *
     * @return First MeSH term
     */
    public String getTerm1() {
        return term1;
    }

    /**
     * Set the first term
     *
     * @return {@code This} for chaining
     */
    public Similarity setTerm1(String term1) {
        this.term1 = term1;
        return this;
    }

    /**
     * Get the second term
     *
     * @return Second MeSH term
     */
    public String getTerm2() {
        return term2;
    }

    /**
     * Set the second term
     *
     * @return {@code This} for chaining
     */
    public Similarity setTerm2(String term2) {
        this.term2 = term2;
        return this;
    }

    /**
     * Get the similarity value of both MeSH terms
     *
     * @return Similarity value
     */
    public double getSimvalue() {
        return simvalue;
    }

    /**
     * Set the similarity value for the MeSH terms
     *
     * @return {@code This} for chaining
     */
    public Similarity setSimvalue(double simvalue) {
        this.simvalue = simvalue;
        return this;
    }

    /**
     * Get the combined key of both MeSH terms ("term1+term2")
     *
     * @return Combined key
     */
    public String getCombinedKey() {
        return term1 + "+" + term2;
    }

    /**
     * Print this similarity object
     *
     * @return String representation
     */
    @Override
    public String toString() {
        return "Similarity{" +
                "term1='" + term1 + '\'' +
                ", term2='" + term2 + '\'' +
                ", simvalue=" + simvalue +
                '}';
    }
}
