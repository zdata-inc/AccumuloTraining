package com.minerkasch.accumulo.examples;

import com.minerkasch.accumulo.examples.util.Constants;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class SparkOfflineUpdate {

  private static final String APP_NAME = "SparkOfflineUpdate";

  public static void main(String[] args)
      throws AccumuloSecurityException, ClassNotFoundException, IOException, AccumuloException,
          TableExistsException, TableNotFoundException {

    // Get a connection to the table and clone the table
    ZooKeeperInstance zoo = new ZooKeeperInstance(Constants.INSTANCE, Constants.ZOOKEEPERS);
    Connector connector =
        zoo.getConnector(Constants.USER_NAME, new PasswordToken(Constants.USER_PASS.getBytes()));

    if (connector.tableOperations().exists(Constants.TWITTER_SPARK_TABLE_CLONE)) {
      connector.tableOperations().delete(Constants.TWITTER_SPARK_TABLE_CLONE);
    }

    connector
        .tableOperations()
        .clone(
            Constants.TWITTER_TABLE,
            Constants.TWITTER_SPARK_TABLE_CLONE,
            false,
            new HashMap<>(),
            new HashSet<>());

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
        Collections.singleton(new Pair<>(new Text("tweet"), new Text("text"))));

    // Set the input table
    InputConfigurator.setInputTableName(
        inputFormatClass, configuration, Constants.TWITTER_SPARK_TABLE_CLONE);

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
    JavaPairRDD<String, Integer> termCount =
        rdd.flatMap(r -> Arrays.asList(r._2.toString().split(" ")).iterator())
            .mapToPair(w -> new Tuple2<>(w, 1))
            .reduceByKey(Integer::sum);

    termCount.take(10).forEach(System.out::println);

    // Convert the data into mutations
    JavaPairRDD<Text, Mutation> mutations =
        termCount.mapToPair(
            row -> {
              Mutation mutation = new Mutation(new Text(row._1));
              mutation.put("", "", Integer.toString(row._2));
              return new Tuple2<>(new Text(Constants.TWITTER_SPARK_TABLE_FREQUENCY), mutation);
            });

    Class<?> outputFormatClass = AccumuloOutputFormat.class;

    // Set the output format
    OutputConfigurator.setZooKeeperInstance(
        outputFormatClass,
        configuration,
        new ClientConfiguration()
            .withInstance(Constants.INSTANCE)
            .withZkHosts(Constants.ZOOKEEPERS));

    // Set the connection settings
    OutputConfigurator.setConnectorInfo(
        outputFormatClass,
        configuration,
        Constants.USER_NAME,
        new PasswordToken(Constants.USER_PASS));

    // Set the output table
    OutputConfigurator.setCreateTables(outputFormatClass, configuration, true);
    OutputConfigurator.setDefaultTableName(
        outputFormatClass, configuration, Constants.TWITTER_SPARK_TABLE_FREQUENCY);

    mutations.saveAsNewAPIHadoopFile(
        "/", Text.class, Mutation.class, AccumuloOutputFormat.class, job.getConfiguration());

    // Delete the cloned table
    connector.tableOperations().delete(Constants.TWITTER_SPARK_TABLE_CLONE);
  }
}
