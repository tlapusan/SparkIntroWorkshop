package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.network.protocol.Encoders;
import org.community.bigdata.workshop.sparkintro.movielens.functions.MovieConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.RatingConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.Movie;
import org.community.bigdata.workshop.sparkintro.movielens.model.Rating;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;

import java.util.List;

/**
 * Created by tudor on 12/6/2015.
 */
public class SparkDriver {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("SparkDriver");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> user_records = sc.textFile("data/movielens/input/users");
        JavaRDD<String> movie_records = sc.textFile("data/movielens/input/movies");
        JavaRDD<String> rating_records = sc.textFile("data/movielens/input/ratings");

        JavaRDD<User> users = user_records.map(new UserConversion());
        JavaRDD<Movie> movies = movie_records.map(new MovieConvertion());
        JavaRDD<Rating> ratings = rating_records.map(new RatingConvertion());

        for (Movie movie : movies.take(10)) {
            System.out.println(movie);
        }

        for (Rating rating : ratings.take(10)) {
            System.out.println("rating : " + rating);
        }

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
