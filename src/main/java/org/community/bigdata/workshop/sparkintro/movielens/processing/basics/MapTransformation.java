package org.community.bigdata.workshop.sparkintro.movielens.processing.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;

/**
 * This class illustrates Spark map transformation and few Spark actions
 * Created by tudor on 12/7/2015.
 */
public class MapTransformation {
    public static void main(String[] args) {

        // initialize spark configuration and context
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(FilterTransformation.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read 'users' file line by line
        JavaRDD<String> records = sc.textFile("data/movielens/input/users");

        // go through each string line and return a new RDD of User objects
        JavaRDD<User> userJavaRDD = records.map(new UserConversion());

        // go through each user and increase its age by one
        JavaRDD<User> userJavaRDDMap = userJavaRDD.map(new Function<User, User>() {
            public User call(User user) throws Exception {
                user.setAge(user.getAge() + 1);
                return user;
            }
        });

        // return the first element from original and incremented RDD
        System.out.println(userJavaRDD.first());
        System.out.println(userJavaRDDMap.first());

        // take n elements from RDD and bring them to main class (driver program)
        for (User user : userJavaRDD.take(10)) {
            System.out.println(user);
        }
        for (User user : userJavaRDDMap.take(10)) {
            System.out.println(user);
        }
    }
}
