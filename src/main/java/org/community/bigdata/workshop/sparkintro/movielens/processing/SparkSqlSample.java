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

        // create an RDD from raw user file
        JavaRDD<String> records = sc.textFile("/Users/tudorl/workspaces/community/bigdata/SparkIntroWorkshop/data/movielens/input/users");
        JavaRDD<User> users = records.map(new UserConversion());

        // create a DataFrame based on user RDD and User class
        DataFrame schemaUser = sqlContext.createDataFrame(users, User.class);

        // register the user tabel
        schemaUser.registerTempTable("user");

        // performs SQL queries on the user table register above
        DataFrame administrators = sqlContext.sql("select * from user where occupation='administrator' sort by age");
        for (Row row : administrators.toJavaRDD().collect()) {
            System.out.println(row);
        }

        System.out.println(administrators.count());
    }
}
