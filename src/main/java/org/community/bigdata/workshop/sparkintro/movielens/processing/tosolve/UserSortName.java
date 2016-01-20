package org.community.bigdata.workshop.sparkintro.movielens.processing.tosolve;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;
import scala.Tuple2;

/**
 * Created by tudorl on 20/01/16.
 */
public class UserSortName {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(MaleFamale.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<User> users = sc.textFile("data/movielens/input/users").map(new UserConversion());

        JavaPairRDD<String, User> usersPairRDD = users.mapToPair(new PairFunction<User, String, User>() {
            @Override
            public Tuple2<String, User> call(User user) throws Exception {
                return new Tuple2<String, User>(user.getOccupation(), user);
            }
        });

        JavaPairRDD<String, User> sortedUsersPairRDD = usersPairRDD.sortByKey();

        for (Tuple2<String, User> userTuple : sortedUsersPairRDD.collect()) {
            System.out.println(userTuple._2());
        }


    }
}
