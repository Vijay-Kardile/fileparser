
package org.fileparser;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

public class simFileReader {

public static final int NUM_PARTITIONS = 1;

  public static class bank implements Serializable {
  	private String name;
  	private int id;

  	public String getName() {
   	 return name;
  	}

  	public void setName(String name) {
    	 this.name = name;
  	}

  	public int getId() {
    	 return id;
   	}

  	public void setId(int id) {
    	 this.id = id;
  	}
  }


  public static class product implements Serializable {
  	private String name;
  	private int id; 
  	private int bankid;	
 
  	public String getName() {
    	 return name;
  	}

  	public void setName(String name) {
    	 this.name = name;
  	}

  	public int getId() {
    	 return id;
  	}

  	public void setId(int id) {
    	 this.id = id;
  	}

  	public int getBankId() {
     	 return bankid;
  	}

  	public void setBankId(int bankid) {
    	 this.bankid = bankid;
  	}

 }

public static class tariff implements Serializable {
        private int prodid;
        private int id;
        private String CRid;
	private String EffDate; 


        public int getProdId() {
         return prodid;
        }

        public void setProdId(int prodid) {
         this.prodid = prodid;
        }

        public int getId() {
         return id;
        }

        public void setId(int id) {
         this.id = id;
        }

        public String getCRId() {
         return CRid;
        }

        public void setCRId(String CRid) {
         this.CRid = CRid;
        }

	public String getEffDate() {
         return EffDate;
        }

        public void setEffDate(String EffDate) {
         this.EffDate = EffDate;
        }

 }

public static void main (String[] args) {

	//readFile(args[0],args[1]);
	//readBuildDataFrame(args[0],args[1]);
	DataFrame2Tables(args[0],args[1]);

    	}

 public static void DataFrame2Tables(String sFile,String tFile)
        {

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

//************************************************************************************88

        JavaRDD<product> prod = sc.textFile(sFile + "prod.txt").map(
                new Function<String, product>() {
                public product call(String line) throws Exception {
                        String[] parts = line.split(",");

                        product  prod = new product();
                        prod.setName(parts[2]);
                        prod.setId(Integer.parseInt(parts[3].trim()));
                        prod.setBankId(Integer.parseInt(parts[4].trim()));

                        return prod;
                }
         });

        Dataset<Row> schemaProduct = sqlContext.createDataFrame(prod, product.class);
        schemaProduct.registerTempTable("product");

//************************************************************************************88

        JavaRDD<bank> obank = sc.textFile(sFile + "bank.txt").map(
                new Function<String, bank>() {
                public bank call(String line) throws Exception {
                        String[] parts = line.split(",");

                        bank  obank = new bank();
                        obank.setName(parts[2]);
                        obank.setId(Integer.parseInt(parts[3].trim()));

                        return obank;
                }
         });

        Dataset<Row> schemaBank = sqlContext.createDataFrame(obank, bank.class);
        schemaBank.registerTempTable("bank");

//************************************************************************************88

        JavaRDD<tariff> otariff = sc.textFile(sFile + "tariff.txt").map(
                new Function<String, tariff>() {
                public tariff call(String line) throws Exception {
                        String[] parts = line.split(",");

                        tariff  otariff = new tariff();
                        otariff.setProdId(Integer.parseInt(parts[2].trim()));
                        otariff.setId(Integer.parseInt(parts[3].trim()));
                        otariff.setCRId(parts[4]);
                        otariff.setEffDate(parts[5]);

                        return otariff;
                }
         });

        Dataset<Row> schemaTariff = sqlContext.createDataFrame(otariff, tariff.class);
        schemaTariff.registerTempTable("tariff");

//************************************************************************************88


// SQL can be run over RDDs that have been registered as tables.
        Dataset<Row> prodDF = sqlContext.sql("SELECT p.id,p.name,b.id,b.name,t.id, t.EffDate FROM product as p, bank as b, tariff t where b.id=p.bankid and t.prodid=p.id");

// The results of SQL queries are DataFrames and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
        JavaRDD<String> lines =  prodDF.javaRDD().map(new Function<Row, String>() {
        public String call(Row row) {
        return "Name: " + row.getString(0);
        }
        }).repartition(NUM_PARTITIONS); 
        
	 prodDF.javaRDD().repartition(NUM_PARTITIONS).saveAsTextFile(tFile+"_prodSQL");
}

//************************************************************************************88


 public static void readBuildDataFrame(String sFile,String tFile)
        {

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
	SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);


	JavaRDD<product> prod = sc.textFile(sFile).map(
  		new Function<String, product>() {
    		public product call(String line) throws Exception {
      			String[] parts = line.split(",");

      			product  prod = new product();
      			prod.setName(parts[0]);
      			prod.setId(Integer.parseInt(parts[1].trim()));

      			return prod;
    		}
	 });

	Dataset<Row> schemaProduct = sqlContext.createDataFrame(prod, product.class);
	schemaProduct.registerTempTable("product");



// SQL can be run over RDDs that have been registered as tables.
	Dataset<Row> prodDF = sqlContext.sql("SELECT name FROM product");

// The results of SQL queries are DataFrames and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
	List<String> prodNames = prodDF.javaRDD().map(new Function<Row, String>() {
  	public String call(Row row) {
    	return "Name: " + row.getString(0);
  	}
	}).collect();

        prodDF.javaRDD().saveAsTextFile(tFile+"_prodSQL");
	}
//************************************************************************************88

	
	public static void readFile(String sFile,String tFile)
	{
	
	SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
	JavaRDD<String> lines = sc.textFile(sFile);
	JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        counts.saveAsTextFile(tFile+"_count");

        JavaRDD<String> bankData = lines.filter(s -> s.contains("BANK"));
        bankData.saveAsTextFile(tFile+"_bank");

        JavaRDD<String> prodData = lines.filter(s -> s.contains("PRODUCT"));
        prodData.saveAsTextFile(tFile+"_prod");

        JavaRDD<String> psxrefData = lines.filter(s -> s.contains("PS_XREF"));
        psxrefData.saveAsTextFile(tFile+"_psxref");

	}	 
}
