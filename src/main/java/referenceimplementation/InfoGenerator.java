package referenceimplementation;

import com.github.javafaker.Faker;

import java.util.Random;

/**
 * Provides the functionality to generate random Info objects with a {@link Faker}.
 */
public class InfoGenerator {

    /**
     * Faker to obtain random names, addresses, ...
     */
    private Faker faker;

    /**
     * Constructor
     *
     * @param faker Faker
     */
    public InfoGenerator(Faker faker) {
        this.faker = faker;
    }

    /**
     * Generate a new Info object with a given ID with the help of the faker
     *
     * @param id ID of the info object
     * @return Newly generated (random) Info object
     */
    public Info generate(int id) {
        String name = faker.name().fullName();
        String address = faker.address().fullAddress();
        Integer age = (new Random(System.nanoTime())).nextInt(120);
        return new Info(id, name, address, age);
    }

}
