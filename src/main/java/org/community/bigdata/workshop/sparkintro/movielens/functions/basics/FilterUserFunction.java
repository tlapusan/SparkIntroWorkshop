package org.community.bigdata.workshop.sparkintro.movielens.functions.basics;

import org.apache.spark.api.java.function.Function;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;

/**
 * Created by tudor on 12/9/2015.
 */
public class FilterUserFunction implements Function<User, Boolean> {
    private String value;

    public FilterUserFunction(String value) {
        this.value = value;
    }

    public Boolean call(User user) throws Exception {
        return user.getOccupation().equals(value);
    }
}
