
/**
 * Illustrates a wordcount in Java
 */
package org.fileparser;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class FileSplitJoin {
  public static void main(String[] args) {

System.out.println("**************************************************************start**********");
    String logFile = args[0]; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    JavaRDD<String> ProdData = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("A"); }
    });

    JavaRDD<String> BankData = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("B"); }
    });




//      logData.saveAsTextFile(args[1]);
//    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    sc.stop();
System.out.println("****************************************************************end******************");
  }
}

