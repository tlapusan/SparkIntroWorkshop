package org.community.bigdata.workshop.sparkintro.movielens.processing.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Create a RDD using an internal Java list.
 *
 * Created by tudorl on 23/01/16.
 */
public class RDDParallelizeSample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[1]").setAppName(RDDParallelizeSample.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // create a List of Characters
        List<Character> characterList = new ArrayList<Character>();
        characterList.addAll(Arrays.asList('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q'));

        // create an RDD from an internal List using parallelize method
        JavaRDD<Character> characterRDD = sc.parallelize(characterList);

        System.out.println("list size : " + characterList.size());
        System.out.println("rdd size : " + characterRDD.count());

        System.out.println("list content : " + characterList);
        System.out.println("rdd content : " + characterRDD.collect());
    }
}
