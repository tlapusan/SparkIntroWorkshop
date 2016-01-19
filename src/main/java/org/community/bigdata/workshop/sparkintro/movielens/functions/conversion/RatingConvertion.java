package org.community.bigdata.workshop.sparkintro.movielens.functions.conversion;

import org.apache.spark.api.java.function.Function;
import org.community.bigdata.workshop.sparkintro.movielens.model.Rating;

/**
 * This function convert a rating string line into a {@link Rating} object
 * <p>
 * Created by tudor on 12/6/2015.
 */
public class RatingConvertion implements Function<String, Rating> {
    public Rating call(String record) throws Exception {
        String[] words = record.split("\t");
        int userId = Integer.valueOf(words[0]);
        int movieId = Integer.valueOf(words[1]);
        float rating = Float.valueOf(words[2]);
        long timestamp = Long.valueOf(words[3]);
        return new Rating(userId, movieId, rating, timestamp);
    }
}
