package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
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

        // create an String RDD from user file
        JavaRDD<String> records = sc.textFile("data/movielens/input/users");

        // create a new RDD with all the words from above RDD
        JavaRDD<String> words = records.flatMap(new WordSeparatorFunction("\\|"));

        // create a paired RDD like (word, 1)
        JavaPairRDD<String, Integer> wordPairs = words.mapToPair(new WordPairFunction());

        // count the frequency for each word (wordcount)
        JavaPairRDD<String, Integer> wordcounts = wordPairs.reduceByKey(new WordcountFunction());

        // persist the RDD
        wordcounts.persist(StorageLevel.MEMORY_AND_DISK());

        // display results
        System.out.println(wordcounts.count());
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
