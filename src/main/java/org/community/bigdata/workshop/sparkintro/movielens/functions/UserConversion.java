package org.community.bigdata.workshop.sparkintro.movielens.functions;

import org.apache.spark.api.java.function.Function;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;

/**
 * Created by tudor on 12/6/2015.
 */
public class UserConversion implements Function<String, User> {
    public User call(String record) throws Exception {
        String[] words = record.split("\\|");
        int id = Integer.valueOf(words[0]);
        int age = Integer.valueOf(words[1]);
        String gender = words[2];
        String occupation = words[3];
        String zipcode = words[4];
        return new User(id, age, gender, occupation, zipcode);
    }
}
