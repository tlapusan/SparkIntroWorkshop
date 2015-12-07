package org.community.bigdata.workshop.sparkintro.movielens.processing.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;

/**
 * Created by tudor on 12/7/2015.
 */
public class MapTransformation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(FilterTransformation.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<User> userJavaRDD = sc.textFile("data/movielens/input/users").map(new UserConversion());
        JavaRDD<User> userJavaRDDMap = userJavaRDD.map(new Function<User, User>() {
            public User call(User user) throws Exception {
                user.setAge(user.getAge() + 1);
                return user;
            }
        });

        System.out.println(userJavaRDD.first());
        System.out.println(userJavaRDDMap.first());
        for (User user : userJavaRDD.take(10)) {
            System.out.println(user);
        }
        for (User user : userJavaRDDMap.take(10)) {
            System.out.println(user);
        }
    }
}
