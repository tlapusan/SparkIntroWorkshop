package org.community.bigdata.workshop.sparkintro.movielens.processing.tosolve;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.MovieConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.RatingConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.model.Movie;
import org.community.bigdata.workshop.sparkintro.movielens.model.Rating;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * Created by tudorl on 15/12/15.
 */
public class MovieRating {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(MovieRating.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Movie> movies = sc.textFile("data/movielens/input/movies").map(new MovieConvertion());
        JavaRDD<Rating> ratings = sc.textFile("data/movielens/input/ratings").map(new RatingConvertion());

        JavaPairRDD<Integer, Movie> moviePairRDD = movies.mapToPair(new PairFunction<Movie, Integer, Movie>() {
            @Override
            public Tuple2<Integer, Movie> call(Movie movie) throws Exception {
                return new Tuple2<Integer, Movie>(movie.getId(), movie);
            }
        });

        JavaPairRDD<Integer, Rating> ratingPairRDD = ratings.mapToPair(new PairFunction<Rating, Integer, Rating>() {
            @Override
            public Tuple2<Integer, Rating> call(Rating rating) throws Exception {
                return new Tuple2<Integer, Rating>(rating.getMovieId(), rating);
            }
        });

        JavaPairRDD<Integer, Tuple2<Movie, Rating>> movieJoin = moviePairRDD.join(ratingPairRDD);

        JavaPairRDD<Integer, Movie> integerMovieJavaPairRDD = movieJoin.groupByKey().mapValues(new Function<Iterable<Tuple2<Movie, Rating>>, Movie>() {
            @Override
            public Movie call(Iterable<Tuple2<Movie, Rating>> movieRating) throws Exception {
                for (Tuple2<Movie, Rating> movieRatingTuple : movieRating) {
                    if (movieRatingTuple._2().getRating() == 3.0) {
                        return movieRatingTuple._1();
                    }
                }
                return null;
            }
        });

        for(Movie movie : integerMovieJavaPairRDD.values().collect()){
            System.out.println(movie);
        }
    }
}
