package org.community.bigdata.workshop.sparkintro.movielens.functions.wordcount;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;

/**
 * Created by tudorl on 07/12/15.
 */
public class WordSeparatorFunction implements FlatMapFunction<String, String> {

    private String delimiter;

    public WordSeparatorFunction(String delimiter){
        this.delimiter = delimiter;
    }

    @Override
    public Iterable<String> call(String s) throws Exception {
        return Arrays.asList(s.split(delimiter));
    }
}
