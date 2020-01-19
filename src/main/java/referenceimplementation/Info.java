package referenceimplementation;

import com.github.javafaker.Faker;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;
import java.util.Random;

/**
 * This is the simple class used for the tests of data distribution without the clustering-based
 * fragmentation but with Apache Ignite's fragmentation strategy. Here, Info and Ill are collocated
 * via the reference of Info.id to Ill.personID (like a Foreign Key reference).
 */
public class Info implements Serializable {

    private static final long serialVerionUID = 3547120985173531458L;


    /**
     * The id of a person is used for collocation of Ill and Info table. Also a queryable field in SQL
     */
    @AffinityKeyMapped
    @QuerySqlField
    private Integer id;

    /**
     * Name of a person (queryable)
     */
    @QuerySqlField
    private String name;

    /**
     * Address of a person (queryable)
     */
    @QuerySqlField
    private String address;

    /**
     * Age of a person (queryable)
     */
    @QuerySqlField
    private Integer age;

//#################### Constructors ####################

    /**
     * Creates a new Info-object with randomized name, address and age with use of {@link Faker} and {@link Random}.
     *
     * @param id ID of a person
     */
    public Info(Integer id) {
        this.id = id;

        Faker faker = new Faker();
        this.name = faker.name().fullName();
        this.address = faker.address().fullAddress();
        Random r = new Random();
        this.age = r.nextInt(120);
    }

    /**
     * Creates a new Info-object with given id, name, address and age.
     *
     * @param id      ID of a person
     * @param name    Name of a person
     * @param address Address of a person
     * @param age     Age of a person
     */
    public Info(Integer id, String name, String address, Integer age) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.age = age;
    }

//#################### Getter & Setter ####################


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

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }


//##################### toString #########################

    /**
     * String representation of the Info object
     *
     * @return String representation
     */
    @Override
    public String toString() {
        String s = "PersonID: " + id + ", Name: " + name + ", Address: " + address + ", Age: " + age;
        return s;
    }

}
