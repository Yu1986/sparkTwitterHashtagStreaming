/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See tthe License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.Tuple2;
import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.*;

import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterFactory;
import twitter4j.Twitter;
import twitter4j.Status;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterException;
import twitter4j.TwitterStreamFactory;
import twitter4j.StatusDeletionNotice;
import twitter4j.StallWarning;
import twitter4j.conf.Configuration;

import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Iterator;
import java.util.Calendar;
import java.text.SimpleDateFormat;

public final class TwitterHashTagCount {
  // set OAuth credentials
  private static void setTwitterOAuth() {
    System.setProperty("twitter4j.oauth.consumerKey", TwitterOAuthKey.consumerKey);
    System.setProperty("twitter4j.oauth.consumerSecret", TwitterOAuthKey.consumerSecret);
    System.setProperty("twitter4j.oauth.accessToken", TwitterOAuthKey.accessToken);
    System.setProperty("twitter4j.oauth.accessTokenSecret", TwitterOAuthKey.accessTokenSecret);
  }

  private static void twitterStreaming(int window, int slide) {

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
    JavaReceiverInputDStream<twitter4j.Status> stream = TwitterUtils.createStream(ssc);

    
    FlatMapFunction<twitter4j.Status, String> mapFunc = new FlatMapFunction<twitter4j.Status, String>(){
      @Override
      public Iterable<String> call(twitter4j.Status status) {
        ArrayList<String> hashTag = new ArrayList<String>();
        Pattern p = Pattern.compile("#(\\w+)\\b");
        Matcher m = p.matcher(status.getText());
        while (m.find()) {
          hashTag.add(m.group(1));
        }
        return hashTag;
      }
    };
    
    PairFunction<String, String, Integer> pairFunc = new PairFunction<String, String, Integer>(){
      @Override
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<String, Integer>(s, 1);
      }
    };

    Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer i1, Integer i2) {
        return i2+i2;
      }
    };

    PairFunction<Tuple2<String, Integer>, Integer, String> swapFunc = new PairFunction<Tuple2<String, Integer>, Integer, String>() {
      @Override
      public Tuple2<Integer, String> call(Tuple2<String, Integer> item) {
        return item.swap();
      }
    };
    
    Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>> sortFunc = new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
      @Override
      public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> rdd) {
        return rdd.sortByKey(false);
      }
    };
    
    VoidFunction<JavaPairRDD<Integer, String>> outFunc = new VoidFunction<JavaPairRDD<Integer, String>>(){
      @Override
      public void call(JavaPairRDD<Integer, String> rdd) {
        List<Tuple2<Integer, String>> list = rdd.take(10);
        Iterator<Tuple2<Integer, String>> ite = list.iterator();
        System.out.println("-------------------------");
        String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
        System.out.println("   "+timeStamp);
        System.out.println("-------------------------");
        while (ite.hasNext()) {
          Tuple2<Integer, String> tag = ite.next();
          System.out.println(tag.toString());
        }
      }
    };
    
    stream.flatMap(mapFunc).mapToPair(pairFunc)
        .reduceByKeyAndWindow(reduceFunc, Durations.seconds(window), Durations.seconds(slide))
        .mapToPair(swapFunc).transformToPair(sortFunc).foreachRDD(outFunc);

    ssc.start();
    ssc.awaitTermination();
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("please input window time and slide time");
      return;
    }
    System.out.println("Windows time = "+args[0]+" seconds");
    System.out.println("Windows slide = "+args[1]+" seconds");
    int window = Integer.parseInt(args[0]);
    int slide = Integer.parseInt(args[1]);

    setTwitterOAuth();
    twitterStreaming(window, slide);
  }
}
