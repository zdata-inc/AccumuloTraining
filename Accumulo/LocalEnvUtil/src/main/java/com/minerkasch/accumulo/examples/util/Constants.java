package com.minerkasch.accumulo.examples.util;

public class Constants {
  // Accumulo Configuration
  public static final String USER_NAME = "root";
  public static final String USER_PASS = "secret";
  public static final String INSTANCE = "instance";
  public static final String ZOOKEEPERS = "";

  // Table names
  public static final String TWITTER_TABLE = "zach.TwitterData";
  public static final String TWITTER_SPARK_TABLE = "zach.SparkTwitterData";
  public static final String TWITTER_SPARK_TABLE_CLONE = "zach.SparkTwitterDataClone";
  public static final String TWITTER_SPARK_TABLE_FREQUENCY = "zach.SparkTwitterDataFrequency";
  public static final String TWEET_INDEX_TABLE = "zach.TweetIndex";
  public static final String TWEET_LOCATION_INDEX_TABLE = "zach.TweetLocationIndex";
}
