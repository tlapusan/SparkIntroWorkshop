package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.wordcount.WordPairFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.wordcount.WordSeparatorFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.wordcount.WordcountFunction;
import scala.Tuple2;

/**
 * Created by tudorl on 07/12/15.
 */
public class SparkWordcount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(SparkWordcount.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> records = sc.textFile("data/movielens/input/users");
        JavaRDD<String> words = records.flatMap(new WordSeparatorFunction("\\|"));
        JavaPairRDD<String, Integer> wordPairs = words.mapToPair(new WordPairFunction());
        JavaPairRDD<String, Integer> wordcounts = wordPairs.reduceByKey(new WordcountFunction());

        for(Tuple2<String, Integer> record : wordcounts.take(100)){
            System.out.println(record._1() + ", " + record._2());
        }
    }
}
