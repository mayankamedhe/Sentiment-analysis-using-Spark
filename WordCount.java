//Set appropriate package name

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//import org.apache.calcite.avatica.ColumnMetaData.StructType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

//import java.awt.List;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.*;
import java.util.*;

import org.apache.spark.sql.Encoders;
/**
 * This class uses RDDs to obtain word count for each word; json files are treated as text file
 * The year-month is obtained as a dataset of String
 * */


public class WordCount {

private static SparkSession spark;

public static void main(String[] args) throws Exception {
		
	BufferedReader entities_file = new BufferedReader( new FileReader("entities.txt") );
	HashSet<String> entities_hash = new HashSet<String>();
	while ( entities_file.ready() ) entities_hash.add( entities_file.readLine() ); // i.e "Ohio"
//	System.out.println("Stored these states in hashset allStates:");
//	for ( String state : entities_hash ) System.out.print( state + " ");
		
	BufferedReader positive_file = new BufferedReader( new FileReader("positive-words.txt") );
	HashSet<String> positive_hash = new HashSet<String>();
	while ( positive_file.ready() ) positive_hash.add( positive_file.readLine() );
	
	BufferedReader negative_file = new BufferedReader( new FileReader("negative-words.txt") );
	HashSet<String> negative_hash = new HashSet<String>();
	while ( negative_file.ready() ) negative_hash.add( negative_file.readLine() );
	
	String inputPath="newsdata"; //Use absolute paths 
	String outputPath="output";   //Use absolute paths
	
	StructType structType = new StructType();
    structType = structType.add("source-name", DataTypes.StringType, false); // false => not nullable
    structType = structType.add("year-month", DataTypes.StringType, false); // false => not nullable
    structType = structType.add("entity", DataTypes.StringType, false); // false => not nullable
    structType = structType.add("sentiment", DataTypes.IntegerType, false); // false => not nullable
    
    ExpressionEncoder<Row> dateRowEncoder = RowEncoder.apply(structType);
    
	SparkSession sparkSession = SparkSession.builder()
			.appName("source-name, year-month, entity, sentiment")		//Name of application
			.master("local")								//Run the application on local node
			.config("spark.sql.shuffle.partitions","4")		//Number of partitions
			.getOrCreate();
	
	//Read multi-line JSON from input files to dataset
			Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath);   
			
			
			// Apply the map function to extract the year-month
			Dataset<Row> data_tuples=inputDataset.flatMap(new FlatMapFunction<Row,Row>(){
				@Override
				public Iterator<Row> call(Row inputDataset) throws Exception {
					
					String source=((String)inputDataset.getAs("source_name"));
					String yearMonthPublished=((String)inputDataset.getAs("date_published")).substring(0, 7);
					String lines=((String)inputDataset.getAs("article_body"));
					Integer sentiment = 0;
					lines = lines.toLowerCase().replaceAll("[^A-Za-z]", " ");  //Remove all punctuation and convert to lower case
					lines = lines.replaceAll("( )+", " ");   //Remove all double spaces
					lines = lines.trim(); 
					List<String> wordList = Arrays.asList(lines.split(" ")); //Get words

					ArrayList<Row> list = new ArrayList<>();
		            Integer flag =0;
					for(int i = 0; i < wordList.size(); i++)
					{
						
						if (entities_hash.contains(wordList.get(i))) 
						{
							for(int j=i-5; j <= i+5; j++)
							{
								if(j >= 0 && j < wordList.size())
								{
									if(positive_hash.contains(wordList.get(j)))
									{
										List<Object> data = new ArrayList<>();
										sentiment = 1;
										data.add(source);
							            data.add(yearMonthPublished);
							            data.add(wordList.get(i));
							            data.add(sentiment);
							            
							            list.add(RowFactory.create(data.toArray()));
							            flag =1;
									}
								
									else if(negative_hash.contains(wordList.get(j)))
									{
										List<Object> data = new ArrayList<>();
										sentiment = -1;
										data.add(source);
							            data.add(yearMonthPublished);
							            data.add(wordList.get(i));
							            data.add(sentiment);
							            
							            list.add(RowFactory.create(data.toArray()));
							            flag =1;
									}
								}
							}
							
							if(flag == 0)
							{
								
								List<Object> data = new ArrayList<>();
								sentiment = 0;
								data.add(source);
					            data.add(yearMonthPublished);
					            data.add(wordList.get(i));
					            data.add(sentiment);
					            
					            list.add(RowFactory.create(data.toArray()));
							}
			            }
						flag =0;
					}
					
					return list.iterator();
					
				}
				
				
			}, dateRowEncoder);
			
			Dataset<Row> count=data_tuples.groupBy("source-name", "year-month", "entity", "sentiment").count().as("count");
					 
			Dataset<Row> support=count.groupBy("source-name", "year-month", "entity").agg(functions.sum(count.col("sentiment").multiply(count.col("count"))).as("senti_sum"), functions.sum(count.col("count")).as("support1"));
			Dataset<Row> filtered = support.filter("support1>=5");
			Dataset<Row> final_dataset = filtered.select("source-name", "year-month", "entity", "senti_sum").orderBy(functions.abs(org.apache.spark.sql.functions.col("senti_sum")).desc());
	
			final_dataset.toJavaRDD().saveAsTextFile(outputPath);	
			
		}
	
}