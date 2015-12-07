package org.community.bigdata.workshop.sparkintro.movielens.functions.wordcount;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by tudorl on 07/12/15.
 */
public class WordcountFunction implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer count1, Integer count2) throws Exception {
        return count1 + count2;
    }
}
