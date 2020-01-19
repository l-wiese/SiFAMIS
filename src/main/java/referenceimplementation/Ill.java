package referenceimplementation;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

/**
 * This is the simple data class for Ill objects, i.e. Ill tuples containing a person's id,
 * a MeSH term (disease) and optionally a Concept Unique Identifier (CUI).
 */
public class Ill implements Serializable {


    private static final long serialVersionUID = -8606827238283310535L;

    /**
     * The ID of a person which is used to collocate Info and Ill objects (queryable).
     */
    @AffinityKeyMapped
    @QuerySqlField(index = true)
    private Integer personID;

    /**
     * The disease this person has/had according to the terms of the MeSH taxonomy (queryable).
     */
    @QuerySqlField
    private String disease;

    /**
     * This is the Concept Unique Identifiers (CUI) of the disease term.
     */
    @QuerySqlField
    private String cui;


//##################### Constructor #########################

    /**
     * Construct an Ill object with given id, MeSH term (disease) and CUI
     *
     * @param personID A person's ID
     * @param disease  MeSH term
     * @param cui      CUI
     */
    public Ill(Integer personID, String disease, String cui) {
        this.personID = personID;
        this.disease = disease;
        this.cui = cui;
    }


//##################### Getter & Setter #########################


    public Integer getPersonID() {
        return personID;
    }

    public void setPersonID(Integer personID) {
        this.personID = personID;
    }

    public String getDisease() {
        return disease;
    }

    public void setDisease(String disease) {
        this.disease = disease;
    }

    public String getCui() {
        return cui;
    }

    public void setCui(String cui) {
        this.cui = cui;
    }


//##################### toString #########################

    /**
     * String representation of an Ill object
     *
     * @return String representation
     */
    @Override
    public String toString() {
        String s = "PersonID: " + personID + ", Disease(MeSH-ID): " + disease + "(" + cui + ")";
        return s;
    }

}
