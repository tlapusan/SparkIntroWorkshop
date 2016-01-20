package org.community.bigdata.workshop.sparkintro.movielens.processing.tosolve;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.basics.MovieYearReleaseFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.MovieConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.model.Movie;

/**
 * Created by tudorl on 20/01/16.
 */
public class MovieYearRelease {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(MovieYearRelease.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> movieRecords = sc.textFile("/Users/tudorl/workspaces/community/bigdata/SparkIntroWorkshop/data/movielens/input/movies");
        JavaRDD<Movie> movieRDD = movieRecords.map(new MovieConvertion());

        JavaRDD<Movie> movieYear = movieRDD.filter(new MovieYearReleaseFunction(95));


        for (Movie movie : movieYear.collect()) {
            System.out.println(movie);
        }


    }
}
