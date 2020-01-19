package materializedfragments;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

/**
 * Class for the ILL table/cache entries.
 */
public class Ill implements Serializable {


    /**
     * Used for partitioning with affinity function {@link clusteringbasedfragmentation.ClusteringAffinityFunction} to collocate Ill and Info tuples.
     * Not queryable as this would not be reasonable!
     */
    @AffinityKeyMapped
    private IllKey key;

    /**
     * This is the Concept Unique Identifier of the disease term.
      */
    @QuerySqlField
    private String cui;

    // Optionally some other attributes ....


//##################### Constructor #########################

    public Ill(IllKey key, String cui) {
        this.key = key;
        this.cui = cui;
    }


//##################### Getter & Setter #########################


    public IllKey getKey() {
        return key;
    }

    public void setKey(IllKey key) {
        this.key = key;
    }




//##################### toString #########################

    @Override
    public String toString() {
        return "PersonID: " + this.key.getId() + ", Disease(MeSH-ID): " + this.key.getDisease() + "("
                + this.cui + ")";
    }

}
