package org.community.bigdata.workshop.sparkintro.movielens.processing.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.community.bigdata.workshop.sparkintro.movielens.functions.basics.FilterUserFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;

/**
 * This class illustrates Spark filter transformation and few Spark actions
 * <p>
 * Created by tudor on 12/7/2015.
 */
public class FilterTransformation {
    public static void main(String[] args) {

        // initialize spark configuration and context
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(FilterTransformation.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read 'users' file line by line and each string line is converted into a User object
        JavaRDD<User> userJavaRDD = sc.textFile("data/movielens/input/users").map(new UserConversion());

        // return a new RDD made from only the Users for each function call return true
        JavaRDD<User> administrators = userJavaRDD.filter(new FilterUserFunction("administrator"));

        // return the administrators RDD as a list to the driver program and display it to console
        for (User user : administrators.collect()) {
            System.out.println(user);
        }

        // display the total number of administrators
        System.out.println(administrators.count());
    }
}
