package org.community.bigdata.workshop.sparkintro.movielens.processing.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * Create a RDD from an external data source.
 *
 * Created by tudorl on 23/01/16.
 */
public class RDDExternalSourceSample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(RDDParallelizeSample.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> userLineRDD = sc.textFile("data/movielens/input/users");

        List<String> userLines = userLineRDD.take(100);
        for (String userLine : userLines) {
            System.out.println(userLine);
        }
    }
}
