package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by tudorl on 19/01/16.
 */
public class SparkWordcountLambda {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(SparkWordcount.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> records = sc.textFile("data/movielens/input/users");
        JavaRDD<String> words = records.flatMap(line -> Arrays.asList(line.split("\\|")));
        JavaPairRDD<String, Integer> wordcounts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((x, y) -> x + y);

        System.out.println(wordcounts.count());
//        wordcounts.saveAsObjectFile("data/movielens/output/2/users_worcount");
        for (Tuple2<String, Integer> record : wordcounts.take(10)) {
            System.out.println(record._1() + ", " + record._2());
        }

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
