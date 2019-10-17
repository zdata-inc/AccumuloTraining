package com.minerkasch.accumulo.examples;

import com.minerkasch.accumulo.examples.util.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;

public class SparkWriteToAccumulo {
  private static final String APP_NAME = "SparkWriter";

  public static void main(String[] args)
      throws ClassNotFoundException, IOException, AccumuloSecurityException {

    // Ensure the user enters the path to the twitter data
    if (args.length != 1) {
      System.out.println("Usage: hadoop jar accumulo-writer-example.jar <data_dir>");
      return;
    }

    // Create the spark conf
    SparkConf conf =
        new SparkConf()
            .setAppName(APP_NAME)
            .setMaster("local[*]")
            .registerKryoClasses(
                new Class<?>[] {
                  Class.forName("org.apache.accumulo.core.data.Key"),
                  Class.forName("org.apache.accumulo.core.data.Value")
                })
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    JavaSparkContext sc = new JavaSparkContext(conf);

    SparkSession spark = SparkSession.builder().appName(APP_NAME).getOrCreate();

    Dataset<Row> df = spark.read().json(args[0]);
    df.createOrReplaceTempView("tweets");
    Dataset<Row> tweets =
        spark.sql("SELECT id, text FROM tweets WHERE text IS NOT NULL AND lang = 'en'");

    // View the data
    tweets.printSchema();
    tweets.show();

    // Convert the data into mutations
    JavaPairRDD<Text, Mutation> mutations =
        tweets
            .toJavaRDD()
            .mapToPair(
                row -> {
                  Mutation mutation = new Mutation(new Text(Long.toString(row.getLong(0))));
                  mutation.put("tweet", "text", row.getString(1));
                  return new Tuple2<>(new Text(Constants.TWITTER_SPARK_TABLE), mutation);
                });


    Job job = new Job();
    Configuration configuration = job.getConfiguration();

    Class<?> inputFormatClass = AccumuloOutputFormat.class;

    // Set the output format
    OutputConfigurator.setZooKeeperInstance(
        inputFormatClass,
        configuration,
        new ClientConfiguration()
            .withInstance(Constants.INSTANCE)
            .withZkHosts(Constants.ZOOKEEPERS));

    // Set the connection settings
    OutputConfigurator.setConnectorInfo(
        inputFormatClass,
        configuration,
        Constants.USER_NAME,
        new PasswordToken(Constants.USER_PASS));

    // Set the output table
    OutputConfigurator.setCreateTables(inputFormatClass, configuration, true);
    OutputConfigurator.setDefaultTableName(
        inputFormatClass, configuration, Constants.TWITTER_SPARK_TABLE);

    mutations.saveAsNewAPIHadoopFile(
        "/", Text.class, Mutation.class, AccumuloOutputFormat.class, job.getConfiguration());
  }
}
