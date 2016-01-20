package org.community.bigdata.workshop.sparkintro.movielens.functions.basics;

import org.apache.spark.api.java.function.Function;
import org.community.bigdata.workshop.sparkintro.movielens.model.Movie;

/**
 * Created by tudorl on 20/01/16.
 */
public class MovieYearReleaseFunction implements Function<Movie, Boolean> {
    private int year;

    public MovieYearReleaseFunction(int year) {
        this.year = year;
    }

    @Override
    public Boolean call(Movie movie) throws Exception {
        if(movie.getReleaseDate() == null){
            return false;
        }
        return movie.getReleaseDate().getYear() == year;
    }
}
