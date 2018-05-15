/**
 * 
 */
package com.zzb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author ZP-209
 *
 */
public class WordCount {
	public static void main(String[] args) {
		String logFile = "word.txt";
//		String logFile = "README.md";
		//sparkSession会比原先的JavaContext包含的信息更多；
		SparkSession spark =SparkSession
				.builder()
				.appName("WordCount")
				.master("local")
				.getOrCreate();
		//获取text中的javaRdd
		JavaRDD<String> lineRdd = spark.sparkContext().textFile(logFile, 1).toJavaRDD();
		//自定义structType
		List<StructField> fields = new ArrayList<StructField>();
		StructField field = DataTypes.createStructField("word", DataTypes.StringType, true);
		fields.add(field);
		StructType schema = DataTypes.createStructType(fields);
		//flatmap进行拆分扁平化，map进行类型转换
		JavaRDD<Row> wordRdd = lineRdd.flatMap((FlatMapFunction<String, String>)record-> {
			return Arrays.asList(record.split(" ")).iterator();
		}).map((Function<String, Row>)record->{
			return RowFactory.create(record);
		});
		//根据schema进行javaRdd到DataSet的转化
		Dataset<Row> wordDataFrame = spark.createDataFrame(wordRdd, schema);
		//进行分组累加
		Dataset<Row> result = wordDataFrame.groupBy("word").count();
		result.show();
	}
}
