package materializedfragments;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Key class for entries of the ILL table/cache
 */
public class IllKey {

    /**
     * Person's ID.
     */
    @QuerySqlField(index = true)
    private Integer id;

    /**
     * Disease string (MeSH term).
     */
    @QuerySqlField
    private String disease;


    /**
     * Constructor for a person ID and a disease string (MeSH term).
     * @param id
     * @param disease
     */
    public IllKey(Integer id, String disease) {
        this.id = id;
        this.disease = disease;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getDisease() {
        return disease;
    }

    public void setDisease(String disease) {
        this.disease = disease;
    }
}
