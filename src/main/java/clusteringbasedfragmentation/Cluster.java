package clusteringbasedfragmentation;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * This class provides the functionality for a clustering of the
 * active domain of the relaxation attribute. A clustering is a set
 * of clusters, e.g. a set of subsets of values that occur in the
 * column of the relaxation attribute.
 * </p>
 * <p>
 * A cluster is identified by it's head element and for all the other
 * values belonging to this cluster there is a similarity value to
 * the head element defined and this value is greater or equal to the
 * in the clustering-procedure defined similarity threshold. The
 * parameter type T is the domain of the relaxation attribute,
 * e.g. a String or an Integer.
 * </p>
 * For the clustering algorithm see documentation of {@link Clustering}.
 */
public class Cluster<T> implements Serializable {

    private static final long serialVersionUID = -4062920907426196146L;

    /**
     * Head element representing the cluster
     */
    private T head;

    /**
     * Subset of values of type T of the active domain that belong
     * to this cluster (Note: does not contain head!)
     */
    private Set<T> adom;


//##################### Constructors ######################

    /**
     * Construct new cluster for given head element and
     * active domain.
     *
     * @param head Head element
     * @param adom Active domain (as Set)
     */
    public Cluster(T head, HashSet<T> adom) {
        this.head = head;
        this.adom = adom;
    }

    /**
     * Construct new cluster for given head element and
     * empty active domain.
     *
     * @param head Head element
     */
    public Cluster(T head) {
        this.head = head;
        adom = new HashSet<>();
    }

//##################### Getter & Setter ######################


    /**
     * Gets the head element of this cluster
     *
     * @return Head element
     */
    public T getHead() {
        return head;
    }

    /**
     * Sets a new head element for this cluster
     *
     * @param head Head element
     */
    public void setHead(T head) {
        this.head = head;
    }


//################### Modify adom-List (Getter/Setter) ##############

    public Set<T> getAdom() {
        return adom;
    }

    public void setAdom(Set<T> adom) {
        this.adom = adom;
    }

//##################### Overwritten Methods #########################

    /**
     * Simple toString method
     *
     * @return String representation of the cluster
     */
    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("Head of the cluster: " + this.head);
        for (Object t : this.adom)
            s.append("\n\tTerm: ").append(t);
        return s.toString();
    }


    /**
     * Compares a cluster to another object. Two clusters
     * are equal if they are identified by the same head element.
     *
     * @param obj Object to compare to
     * @return True if {@code this} and the {@link Object} {@code obj} are equal,
     * else false is returned
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Cluster))
            return false;

        // Compare head elements
        Cluster other = (Cluster) obj;
        return this.head.equals(other.head);
    }

}
