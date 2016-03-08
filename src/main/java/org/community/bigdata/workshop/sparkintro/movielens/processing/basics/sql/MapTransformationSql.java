package org.community.bigdata.workshop.sparkintro.movielens.processing.basics.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;

/**
 * Created by tudorl on 08/03/16.
 */
public class MapTransformationSql {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(MapTransformationSql.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> records = sc.textFile("data/movielens/input/users");

        JavaRDD<User> users = records.map(new UserConversion());

        DataFrame userFrame = sqlContext.createDataFrame(users, User.class);

        // using the DataFrame API to increase the age of users by one
        DataFrame userIncrAge = userFrame.select(userFrame.col("id"), userFrame.col("age").plus(1));
        for (Row row : userIncrAge.take(10)) {
            System.out.println(row);
        }

        // using Spark SQL to increase the age of users by one
        userFrame.registerTempTable("users");
        DataFrame userIncrAge2 = sqlContext.sql("select id, age + 1 from users limit 10");
        System.out.println("----");
        for (Row row : userIncrAge2.collect()) {
            System.out.println(row);
        }
    }
}
