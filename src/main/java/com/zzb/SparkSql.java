/**
 * 
 */
package com.zzb;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * @author ZP-209
 *
 */
public class SparkSql {
	public static void main(String[] args) {
		String logFile = "README.md";
		String jsonFile = "people.json";
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL basic example")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> df = spark.read().json(jsonFile);
		df.printSchema();
		df.select("name").show();
		df.select(col("name"),col("age").plus(1)).show();
		df.groupBy("age").count().show();
		df.createOrReplaceTempView("people");
		Dataset<Row> SqlDf = spark.sql("SELECT * FROM people");
		SqlDf.show();
		
		Person person = new Person();
		person.setName("Andy");
		person.setAge(13);
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(
				Collections.singletonList(person), 
				personEncoder);
		
		javaBeanDS.show();
		
		String schemaString = "name age";
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		
		
		
		
	} 
	public static class Person implements Serializable{
		private String name;
		private int age;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public int getAge() {
			return age;
		}
		public void setAge(int age) {
			this.age = age;
		}
	}
	public static class Employee implements Serializable{
		
	}
	public static class Average implements Serializable{
		
	}
	//自定义聚合函数，使用泛型保证数据安全
	public static class MyAverage extends Aggregator<Employee, Average, Double>{


		public Average zero() {
			return new Average();
		}

		@Override
		public Encoder<Average> bufferEncoder() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Double finish(Average arg0) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Average merge(Average arg0, Average arg1) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Encoder<Double> outputEncoder() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Average reduce(Average arg0, Employee arg1) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	//无类型的自定义聚合函数
	public static class MyAverage2 extends UserDefinedAggregateFunction{
		/**
		 * 
		 */
		@Override
		public StructType bufferSchema() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public DataType dataType() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean deterministic() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public Object evaluate(Row arg0) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void initialize(MutableAggregationBuffer arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public StructType inputSchema() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void merge(MutableAggregationBuffer arg0, Row arg1) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void update(MutableAggregationBuffer arg0, Row arg1) {
			// TODO Auto-generated method stub
			
		}
		
	}
}

