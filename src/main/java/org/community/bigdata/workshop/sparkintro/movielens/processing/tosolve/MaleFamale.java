package org.community.bigdata.workshop.sparkintro.movielens.processing.tosolve;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;
import scala.Tuple2;

/**
 * Created by tudorl on 15/12/15.
 */
public class MaleFamale {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(MaleFamale.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<User> users = sc.textFile("data/movielens/input/users").map(new UserConversion());


        JavaRDD<User> male = users.filter(new Function<User, Boolean>() {
            @Override
            public Boolean call(User user) throws Exception {
                return user.getGender().equals("M");
            }
        });

        JavaRDD<User> female = users.filter(new Function<User, Boolean>() {
            @Override
            public Boolean call(User user) throws Exception {
                return user.getGender().equals("F");
            }
        });

        JavaPairRDD<Integer, Integer> malePair = male.mapToPair(new PairFunction<User, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(User user) throws Exception {
                return new Tuple2<Integer, Integer>(user.getAge(), 1);
            }
        });

        JavaPairRDD<Integer, Integer> maleDistribution = malePair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        for (Tuple2<Integer, Integer> maleDist : maleDistribution.collect()) {
            System.out.println("male age, dist : " + maleDist._1() + ", " + maleDist._2());
        }


//        for (User user : male.collect()) {
//            System.out.println("M " + user);
//        }
//        System.out.println("---------------");
//        for (User user : female.collect()) {
//            System.out.println("F " + user);
//        }

        System.out.println("Total users : " + users.count());
        System.out.println("Total male : " + male.count());
        System.out.println("Total female : " + female.count());


    }
}
