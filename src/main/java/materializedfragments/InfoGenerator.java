package materializedfragments;

import com.github.javafaker.Faker;

import java.util.Random;

/**
 * Class that provides the random INFO entry generation.
 * For details see {@link Faker}
 */
public class InfoGenerator {

    /**
     * Faker that provides the random values.
     */
    private Faker faker;

    /**
     * Constructor with a faker.
     * @param faker
     */
    public InfoGenerator(Faker faker) {
        this.faker = faker;
    }

    /**
     * Generate a random INFO table/cache entry (i.e. an {@link Info} object) with a given id.
     * @param id ID of the person
     * @return Random INFO table/cache entry
     */
    public Info generate(Integer id) {
        String name = faker.name().fullName();
        String address = faker.address().fullAddress();
        Integer age = (new Random(System.nanoTime())).nextInt(120);
        return new Info(new FragIDKey(-1, id), name, address, age);
    }

}
