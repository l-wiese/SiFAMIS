package materializedfragments;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

/**
 * Class for the INFO table/cache entries.
 */
public class Info implements Serializable {

    /**
     * Key
     */
    @AffinityKeyMapped
    private FragIDKey key;

    /**
     * Name of person
     */
    @QuerySqlField
    private String name;

    /**
     * Address of person
     */
    @QuerySqlField
    private String address;

    /**
     * Age of person
     */
    @QuerySqlField
    private int age;

    // Optionally some more attributes

//#################### Constructors ####################

    /**
     * Constructor for an INFO entry containing a key and the person's name, address and age.
     * @param key Key
     * @param name Person's name
     * @param address Person's address
     * @param age Person's age
     */
    public Info(FragIDKey key, String name, String address, int age) {
        this.key = key;
        this.name = name;
        this.address = address;
        this.age = age;
    }


//#################### Getter & Setter ####################


    public FragIDKey getKey() {
        return key;
    }

    public void setKey(FragIDKey key) {
        this.key = key;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

//##################### toString #########################

    @Override
    public String toString() {
        String s = "PersonID: " + this.key.getId() + ", Name: " + name + ", Address: " + address + ", Age: " + age;
        return s;
    }

}
