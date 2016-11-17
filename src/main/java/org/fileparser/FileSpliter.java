/**
 * Illustrates a wordcount in Java
 */
package org.fileparser;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class FileSpliter {
  public static void main(String[] args) {

System.out.println("**************************************************************start**********");
    String logFile = args[0]; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("A"); }
    }).saveAsTextFile(args[1] + "_A");

    logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("B"); }
    }).saveAsTextFile(args[1] + "_B");


//	logData.saveAsTextFile(args[1]);
//    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    
    sc.stop();
System.out.println("****************************************************************end******************");
  }
}
