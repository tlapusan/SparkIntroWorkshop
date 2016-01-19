package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;

/**
 * Created by tudorl on 19/01/16.
 */
public class SparkSqlSample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(SparkSqlSample.class.getSimpleName()).setMaster("local[4]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> records = sc.textFile("/Users/tudorl/workspaces/community/bigdata/SparkIntroWorkshop/data/movielens/input/users");
        JavaRDD<User> users = records.map(new UserConversion());

        DataFrame schemaUser = sqlContext.createDataFrame(users, User.class);
        schemaUser.registerTempTable("user");

        DataFrame administrators = sqlContext.sql("select * from user where occupation='administrator' sort by age");
        for (Row row : administrators.toJavaRDD().collect()) {
            System.out.println(row);
        }

        System.out.println(administrators.count());
    }
}
