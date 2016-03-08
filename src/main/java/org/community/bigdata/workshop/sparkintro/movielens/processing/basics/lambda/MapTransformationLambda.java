package org.community.bigdata.workshop.sparkintro.movielens.processing.basics.lambda;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;

/**
 * Created by tudorl on 08/03/16.
 */
public class MapTransformationLambda {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(MapTransformationLambda.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read 'users' file line by line
        JavaRDD<String> records = sc.textFile("data/movielens/input/users");

        // go through each string line and return a new RDD of User objects
        JavaRDD<User> userJavaRDD = records.map(line -> new UserConversion().call(line));

        // using lambda expression to increment the age of users by one
        JavaRDD<User> userIncrAge = userJavaRDD.map(user -> new User(user.getId(), user.getAge(), user.getGender(), user.getOccupation(), user.getZipcode()));

        // persist the RDD
        userJavaRDD.persist(StorageLevel.MEMORY_AND_DISK());
        userIncrAge.persist(StorageLevel.MEMORY_AND_DISK());

        // return the first element from original and incremented RDD
        System.out.println(userJavaRDD.first());
        System.out.println(userIncrAge.first());

        // take n elements from RDD and bring them to main class (driver program)
        for (User user : userJavaRDD.take(10)) {
            System.out.println(user);
        }
        for (User user : userIncrAge.take(10)) {
            System.out.println(user);
        }

        try {
            Thread.sleep(1000 * 60 * 10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
