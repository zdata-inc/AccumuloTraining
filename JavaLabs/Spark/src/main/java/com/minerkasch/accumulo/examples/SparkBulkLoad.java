package com.minerkasch.accumulo.examples;

import com.minerkasch.accumulo.examples.util.Constants;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

public class SparkBulkLoad {

  private static final String APP_NAME = "SparkBulkLoad";

  public static void main(String[] args)
      throws ClassNotFoundException, IOException, AccumuloSecurityException, AccumuloException,
          TableExistsException, TableNotFoundException, URISyntaxException {

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
    JavaPairRDD<Key, Value> kvp =
        tweets
            .toJavaRDD()
            .mapToPair(
                row ->
                    new Tuple2<>(
                        new Key(Long.toString(row.getLong(0)), "tweet", "text"),
                        new Value(row.getString(1))));

    JavaPairRDD<Key, Value> sortedKvp = kvp.sortByKey();

    FileSystem hdfs =
        FileSystem.get(
            new URI("hdfs://ec2-18-191-210-82.us-east-2.compute.amazonaws.com:8020"),
            sc.hadoopConfiguration());

    Path tmpDir = new Path("/tmp/" + UUID.randomUUID());
    hdfs.mkdirs(tmpDir, new FsPermission("777"));
    Path dataDir = new Path(tmpDir.toString() + "/data");
    Path failDir = new Path(tmpDir.toString() + "/fail");
    hdfs.mkdirs(failDir);

    sortedKvp.saveAsNewAPIHadoopFile(
            "hdfs://ec2-18-191-210-82.us-east-2.compute.amazonaws.com:8020" + dataDir.toString(), Key.class, Value.class, AccumuloFileOutputFormat.class);

    hdfs.setPermission(dataDir, new FsPermission("777"));
    hdfs.setPermission(failDir, new FsPermission("777"));

    // Get a connection to the table
    ZooKeeperInstance zoo = new ZooKeeperInstance(Constants.INSTANCE, Constants.ZOOKEEPERS);
    Connector connector =
        zoo.getConnector(Constants.USER_NAME, new PasswordToken(Constants.USER_PASS.getBytes()));

    if (connector.tableOperations().exists(Constants.TWITTER_SPARK_TABLE)) {
      connector.tableOperations().delete(Constants.TWITTER_SPARK_TABLE);
    }
    connector.tableOperations().create(Constants.TWITTER_SPARK_TABLE);

    // Import the data
    connector
        .tableOperations()
        .importDirectory(
            Constants.TWITTER_SPARK_TABLE, dataDir.toString(), failDir.toString(), true);

    hdfs.delete(tmpDir, true);
  }
}
