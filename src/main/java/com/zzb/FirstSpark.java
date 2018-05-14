/**
 * 
 */
package com.zzb;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

/**
 * @author ZP-209
 *
 */
public class FirstSpark {

	public static void main(String[] args) {
		// 配置spark的启动信息，创建spark重要对象JavaSparkContext
		SparkConf conf = new SparkConf().setMaster("local").setAppName("FirstSpark");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// 通过parallelize的方式得到JavaRDD对象
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> disDataRDD = jsc.parallelize(data);
		disDataRDD.reduce((a, b) -> a + b);
		JavaRDD<String> textData = jsc.textFile("README.md");
		textData.map(s -> s.length()).reduce((a, b) -> a + b);
		textData.mapToPair(s -> new Tuple2<>(s, 1));
		Broadcast<int[]> broadcastVar = jsc.broadcast(new int[] {1,2,3});
		broadcastVar.value();
		LongAccumulator accumulator = jsc.sc().longAccumulator();
		
	}

}
