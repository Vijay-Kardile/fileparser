/**
 * Illustrates a wordcount in Java
 */
/*
A,HDR TBLE,BANK,R1
A,BANK,1001,BANK1,1,1,1,1,1,1,1,1,1,1,1,1
A,BANK,1002,BANK2,5,5,5,5,5,5,5,5,5,5,5,5
A,TLR TBLE,BANK,2,R1
A,HDR TBLE,PS_XREF,R1
A,PS_XREF,P1,1001,1,1,1,1,1,1,1,1,1,1,1
A,PS_XREF,P2,1001,1,1,1,1,1,1,1,1,1,1,1
A,PS_XREF,P3,1002,3,3,3,3,3,3,3,3,3,3,3
A,HDR TBLE,PS_XREF,R1
A,HDR TBLE,PRODUCT,R1
A,PRODUCT,P1,1,1,1,1,1,1,1,1,1,1,1
A,PRODUCT,P2,1,1,1,1,1,1,1,1,1,1,1
A,PRODUCT,P3,3,3,3,3,3,3,3,3,3,3,3
A,HDR TBLE,PRODUCT,R1
*/
package org.fileparser;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class FileSplitJoinBackup {
  public static void main(String[] args) {

	System.out.println("**************************************************************start**********");
	System.out.println("*********************************************PRODUCT");
    	String logFile = args[0]; // Should be some file on your system
    	SparkConf conf = new SparkConf().setAppName("Simple Application");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	JavaRDD<String> logData = sc.textFile(logFile).cache();

    	JavaRDD<String> prodData = logData.filter(new Function<String, Boolean>() {
      		public Boolean call(String s) { return s.contains("PRODUCT"); }
    		});

       	JavaPairRDD<String, String> prodPairs = prodData.mapToPair(new PairFunction<String, String, String>() {
            	public Tuple2<String, String> call(String s) {
                	String[] prodSplit = s.split(",");
                return new Tuple2<String, String>(prodSplit[2], prodSplit[0]+","+prodSplit[1]+","+prodSplit[2]); //+","+prodSplit[3]+","+prodSplit[4]+","+prodSplit[5]);
            	}});//.distinct();

	System.out.println("*********************************************BANK");
    	JavaRDD<String> bankData = logData.filter(new Function<String, Boolean>() {
      		public Boolean call(String s) { return s.contains("BANK"); }
    		});

       	JavaPairRDD<String, String> bankPairs = bankData.mapToPair(new PairFunction<String, String, String>() {
            	public Tuple2<String, String> call(String s) {
                	String[] bankSplit = s.split(",");
                return new Tuple2<String, String>(bankSplit[2], bankSplit[0]+","+ bankSplit[1]+","+ bankSplit[2]); //+","+ bankSplit[3]+","+ bankSplit[4]);
            	}});//.distinct();

	System.out.println("*********************************************PS XREF");
    	JavaRDD<String> ps_xrefData = logData.filter(new Function<String, Boolean>() {
      		public Boolean call(String s) { return s.contains("PS_XREF"); }
    		});

       	JavaPairRDD<String, String> ps_xrefPairs = ps_xrefData.mapToPair(new PairFunction<String, String, String>() {
            	public Tuple2<String, String> call(String s) {
                	String[] ps_xrefSplit = s.split(",");
                return new Tuple2<String, String>(ps_xrefSplit[3], ps_xrefSplit[0]+","+ ps_xrefSplit[1]+","+ ps_xrefSplit[2]); //+","+ ps_xrefSplit[3]+","+ ps_xrefSplit[4]);
            	}});//.distinct();
	System.out.println("*********************************************test");

	//JavaRDD<String> lines = sc.textFile(logFile);
	//JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
	//JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
	System.out.println("*********************************************JOIN");

	JavaPairRDD<String, Tuple2<String, String>> joinsOutput = ps_xrefPairs.join(bankPairs);
	System.out.println("Joins function Output: "+joinsOutput.collect());
	

	prodPairs.saveAsTextFile(args[1]+"_prod");
	bankPairs.saveAsTextFile(args[1]+"_bank");
	ps_xrefPairs.saveAsTextFile(args[1]+"_psxref");
	joinsOutput.saveAsTextFile(args[1]+"_join");

		
//	logData.saveAsTextFile(args[1]);
//    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    
    	sc.stop();
	System.out.println("****************************************************************end******************");
  }
}
