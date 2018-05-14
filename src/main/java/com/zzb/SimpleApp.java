/**
 * 
 */
package com.zzb;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author ZP-209
 *
 */
public class SimpleApp {
	public static void main(String[] args) {
		String logFile = "README.md";
		SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();

		Dataset<String> logData = spark.read().textFile(logFile).cache();
		
		long numAs = logData.filter(o->o.contains("a")).count();
		long numBs = logData.filter(o->o.contains("b")).count();
		
		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		
		spark.stop();
	}
}
