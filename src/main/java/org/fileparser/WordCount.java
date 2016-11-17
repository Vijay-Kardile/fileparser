/**
 * Illustrates a wordcount in Java
 */
package org.fileparser;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class WordCount {
  public static void main(String[] args) {

System.out.println("**************************************************************start**********");
    String logFile = args[0]; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

	logData.saveAsTextFile(args[1]);
    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    
    sc.stop();
System.out.println("****************************************************************end******************");
  }
}
