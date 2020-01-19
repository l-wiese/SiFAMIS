package referenceimplementation;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Data class for treat objects
 */
public class Treat implements Serializable {

    private static final long serialVersionUID = 6349440452151396341L;

    /**
     * The id of a person is used for collocation. Also a queryable field in SQL.
     */
    @AffinityKeyMapped
    @QuerySqlField
    private Integer id;


    /**
     * Prescription.
     */
    @QuerySqlField
    private String prescription;


    /**
     * Constructor for Treatment
     * @param id Patient ID
     * @param prescription Prescription (may be null)
     */
    public Treat(@NotNull Integer id, String prescription) {
        this.id = id;
        this.prescription = prescription;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getPrescription() {
        return prescription;
    }

    public void setPrescription(String prescription) {
        this.prescription = prescription;
    }

    @Override
    public String toString() {
        return "Treat{Patient ID=" + id +", Prescription='" + prescription + "'}";
    }
}
