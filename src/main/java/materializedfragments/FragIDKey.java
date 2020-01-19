package materializedfragments;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

public class FragIDKey implements Serializable {

    private static final long serialVersionUID = -4266067583137195303L;


    private int fragID;

    @QuerySqlField
    private int id;

    public FragIDKey(int fragID, int id) {
        this.fragID = fragID;
        this.id = id;
    }

    public int getFragID() {
        return fragID;
    }

    public void setFragID(int fragID) {
        this.fragID = fragID;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
