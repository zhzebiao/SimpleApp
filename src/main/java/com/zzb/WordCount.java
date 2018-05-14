/**
 * 
 */
package com.zzb;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author ZP-209
 *
 */
public class WordCount {
	public static void main(String[] args) {
		String logFile = "README.md";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCount");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		JavaRDD<String> lines = jsc.textFile(logFile);
		lines.map(o->Row(o))
	}
}
