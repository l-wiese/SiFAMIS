package materializedfragments;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

/**
 * Class for the TREAT table/cache entries.
 */
public class Treat implements Serializable {

    private static final long serialVersionUID = -2598294928805106955L;

    /**
     * Key.
     */
    @AffinityKeyMapped
    private FragIDKey key;


    /**
     * Prescription.
     */
    @QuerySqlField
    private String prescription;


    /**
     * Prescription was successful or not. Null if not yet applied or finished. This is a dummy column, that is
     * required in ignite as the patient id and the prescription form the primary key of the table, and the table
     * must have some non primary key column...
     */
    @QuerySqlField
    private Boolean success;

    /**
     * Constructor for Treatment
     * @param key Fragment ID key
     * @param prescription Prescription (may be null)
     */
    public Treat(FragIDKey key, String prescription, Boolean success) {
        this.key = key;
        this.prescription = prescription;
        this.success = success;
    }

    public FragIDKey getKey() {
        return key;
    }

    public void setKey(FragIDKey key) {
        this.key = key;
    }

    public String getPrescription() {
        return prescription;
    }

    public void setPrescription(String prescription) {
        this.prescription = prescription;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    @Override
    public String toString() {
        return "Treat{Patient ID=" + key.getId() +", Prescription='" + prescription + "'}";
    }
}

