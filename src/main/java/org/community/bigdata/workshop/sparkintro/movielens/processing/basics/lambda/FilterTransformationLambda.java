package org.community.bigdata.workshop.sparkintro.movielens.processing.basics.lambda;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;

/**
 * Created by tudorl on 08/03/16.
 */
public class FilterTransformationLambda {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(FilterTransformationLambda.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read 'users' file line by line and each string line is converted into a User object.
        // bellow are presented three ways of creating the user RDD

//        JavaRDD<User> userJavaRDD = sc.textFile("data/movielens/input/users").map(new UserConversion());
//        JavaRDD<User> userJavaRDD = sc.textFile("data/movielens/input/users").map(line -> new UserConversion().call(line));
        JavaRDD<User> userJavaRDD = sc.textFile("data/movielens/input/users").map(line -> line.split("\\|")).
                map(words -> new User(Integer.valueOf(words[0]), Integer.valueOf(words[1]), words[2], words[3], words[4]));

        // return a new RDD made from only the Users for each function call return true
        JavaRDD<User> administrators = userJavaRDD.filter(user -> user.getOccupation().equals("administrator"));

        // persist the RDD
        administrators.persist(StorageLevel.MEMORY_AND_DISK());

        // return the administrators RDD as a list to the driver program and display it to console
        for (User user : administrators.collect()) {
            System.out.println(user);
        }

        // display the total number of administrators
        System.out.println(administrators.count());

        try {
            Thread.sleep(1000 * 60 * 10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
