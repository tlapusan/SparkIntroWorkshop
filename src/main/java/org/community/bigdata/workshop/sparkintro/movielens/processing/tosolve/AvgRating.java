package org.community.bigdata.workshop.sparkintro.movielens.processing.tosolve;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.RatingConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.model.Rating;
import scala.Tuple2;

/**
 * Created by tudorl on 15/12/15.
 */
public class AvgRating {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(AvgRating.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Rating> ratings = sc.textFile("data/movielens/input/ratings").map(new RatingConvertion());
        JavaPairRDD<Integer, Float> ratingPairs = ratings.mapToPair(new PairFunction<Rating, Integer, Float>() {
            @Override
            public Tuple2<Integer, Float> call(Rating rating) throws Exception {
                return new Tuple2<Integer, Float>(rating.getMovieId(), rating.getRating());
            }
        });

        JavaPairRDD<Integer, Float> avgMovieRating = ratingPairs.groupByKey().mapValues(new Function<Iterable<Float>, Float>() {
            @Override
            public Float call(Iterable<Float> ratings) throws Exception {
                int count = 0;
                float sum = 0;
                for (Float rating : ratings) {
                    count++;
                    sum += rating;
                }
                return sum / count;
            }
        });

        for (Tuple2<Integer, Float> movieRating : avgMovieRating.collect()) {
            System.out.println("user id, rating : " +
                    movieRating._1() + ", " +movieRating._2());
        }

    }
}
