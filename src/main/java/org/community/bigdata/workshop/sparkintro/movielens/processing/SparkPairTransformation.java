package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.MovieConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.RatingConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.Movie;
import org.community.bigdata.workshop.sparkintro.movielens.model.Rating;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;
import scala.Tuple2;

/**
 * Created by tudor on 12/6/2015.
 */
public class SparkPairTransformation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("SparkPairTransformation");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> user_records = sc.textFile("data/movielens/input/users");
        JavaRDD<String> movie_records = sc.textFile("data/movielens/input/movies");
        JavaRDD<String> rating_records = sc.textFile("data/movielens/input/ratings");

        JavaRDD<User> users = user_records.map(new UserConversion());
        JavaRDD<Movie> movies = movie_records.map(new MovieConvertion());
        JavaRDD<Rating> ratings = rating_records.map(new RatingConvertion());

        JavaPairRDD<Integer, User> userPairRDD = users.mapToPair(new PairFunction<User, Integer, User>() {
            @Override
            public Tuple2<Integer, User> call(User user) throws Exception {
                return new Tuple2<Integer, User>(user.getId(), user);
            }
        });
        JavaPairRDD<Integer, Rating> ratingPairRDD = ratings.mapToPair(new PairFunction<Rating, Integer, Rating>() {
            @Override
            public Tuple2<Integer, Rating> call(Rating rating) throws Exception {
                return new Tuple2<Integer, Rating>(rating.getUserId(), rating);
            }
        });

        userPairRDD.join(ratingPairRDD);
        JavaPairRDD<Integer, Tuple2<User, Rating>> join = userPairRDD.join(ratingPairRDD);
        for (Tuple2<Integer, Tuple2<User, Rating>> tuple2 : join.take(1000)) {
            System.out.println(tuple2._1() + ", " + tuple2._2()._1() + ", " + tuple2._2()._2());
        }
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
