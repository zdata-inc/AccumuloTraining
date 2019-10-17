package com.minerkasch.accumulo.examples;

import com.minerkasch.accumulo.examples.util.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Collections;

public class SparkReadFromAccumulo {

  private static final String APP_NAME = "SparkReader";

  public static void main(String[] args)
      throws ClassNotFoundException, AccumuloSecurityException, IOException {

    Job job = new Job();
    Configuration configuration = job.getConfiguration();

    Class<?> inputFormatClass = AccumuloInputFormat.class;

    // Set the input format
    InputConfigurator.setZooKeeperInstance(
        inputFormatClass,
        configuration,
        new ClientConfiguration()
            .withInstance(Constants.INSTANCE)
            .withZkHosts(Constants.ZOOKEEPERS));

    // Set the connection settings
    InputConfigurator.setConnectorInfo(
        inputFormatClass,
        configuration,
        Constants.USER_NAME,
        new PasswordToken(Constants.USER_PASS));

    // Set the users scan authorizations
    InputConfigurator.setScanAuthorizations(inputFormatClass, configuration, new Authorizations());

    // Filter on the column family
    InputConfigurator.fetchColumns(
        inputFormatClass,
        configuration,
        Collections.singleton(new Pair<>(new Text("tweet"), null)));

    // Set the input table
    InputConfigurator.setInputTableName(inputFormatClass, configuration, Constants.TWITTER_TABLE);

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

    // Retrieve data from Accumulo
    JavaPairRDD<Key, Value> rdd =
        sc.newAPIHadoopRDD(
            job.getConfiguration(), AccumuloInputFormat.class, Key.class, Value.class);

    // Display the number of records
    System.out.println("Num records: " + rdd.count());

    // Filter and display only the text from 10 of the tweets
    rdd.filter(r -> r._1.getColumnQualifier().equals(new Text("text")))
        .map(r -> r._2.toString())
        .take(10)
        .forEach(System.out::println);
  }
}
