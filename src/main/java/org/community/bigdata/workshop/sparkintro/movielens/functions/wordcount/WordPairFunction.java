package org.community.bigdata.workshop.sparkintro.movielens.functions.wordcount;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by tudorl on 07/12/15.
 */
public class WordPairFunction implements PairFunction<String, String, Integer> {
    @Override
    public Tuple2<String, Integer> call(String word) throws Exception {
        return new Tuple2<String, Integer>(word, 1);
    }
}
