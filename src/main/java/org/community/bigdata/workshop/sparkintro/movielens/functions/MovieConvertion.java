package org.community.bigdata.workshop.sparkintro.movielens.functions;

import org.apache.spark.api.java.function.Function;
import org.community.bigdata.workshop.sparkintro.movielens.model.Movie;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This function convert a movie string line into a {@link Movie} object
 *
 * Created by tudor on 12/6/2015.
 */
public class MovieConvertion implements Function<String, Movie> {
    public Movie call(String record) throws Exception {
        String[] words = record.split("\\|");
        int id = Integer.valueOf(words[0]);
        String movieTitle = words[1];
        Date releaseDate = convertStringToDate(words[2]);
        String url = words[4];
        String genres = words[5];
        return new Movie(id, movieTitle, releaseDate, null, url, genres);
    }

    private Date convertStringToDate(String value) {
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
        try {
            return formatter.parse(value);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
