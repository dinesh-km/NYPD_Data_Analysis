package com.na.com;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataAnalysis
{
	StringBuilder str = new  StringBuilder();
	public static void main(String[] args) throws Exception
	{
		
		
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "NYPD Analyse");
		job.setJarByClass(DataAnalysis.class);
		job.setMapperClass(MapperTask.class);
		job.setReducerClass(ReducerTask.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
	}


class MapperTask extends Mapper <LongWritable, Text, Text , IntWritable>
{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		
		String token=value.toString();
		String[] tokens=token.split(",");
		
		if(tokens[0]!="#DATE") {
			
			//1. Date on which maximum number of accidents took place.
			String key1 = "one" + tokens[0];
			
			context.write(new Text(key1.toString()), new IntWritable(1));
			
			//2. Borough with maximum count of accident fatality
			String key2 = "two" + tokens[1];
			int personsFatality = 	Integer.parseInt(tokens[4])
								  + Integer.parseInt(tokens[6])
								  + Integer.parseInt(tokens[8])
								  + Integer.parseInt(tokens[10]);

			context.write(new Text(key2), new IntWritable(personsFatality));
			
			//3. Zip with maximum count of accident fatality
			String key3 = "thr" + tokens[2];
			int zipPersonsFatality = 	Integer.parseInt(tokens[4])
									  + Integer.parseInt(tokens[6])
									  + Integer.parseInt(tokens[8])
									  + Integer.parseInt(tokens[10]);
			context.write(new Text(key3), new IntWritable(zipPersonsFatality));
			
			//4. Which vehicle type is involved in maximum accidents
			String key4 = "fou" + tokens[11];
			context.write(new Text(key4), new IntWritable(1));
			
			String[] year = tokens[0].split("/");
			
			//5. Year in which maximum Number Of Persons and Pedestrians Injured
			String key5 = "fiv" + year[2];
			int peoplePedestriansInjured = 	  Integer.parseInt(tokens[3])
											+ Integer.parseInt(tokens[5]);
			context.write(new Text(key5), new IntWritable(peoplePedestriansInjured));
			
			//6. Year in which maximum Number Of Persons and Pedestrians Killed
			String key6 = "six" + year[2];
			int peoplePedestriansKilled = 	  Integer.parseInt(tokens[4])
											+ Integer.parseInt(tokens[6]);
			context.write(new Text(key6), new IntWritable(peoplePedestriansKilled));
			
			//7. Year in which maximum Number Of Cyclist Injured and Killed (combined)
			String key7 = "sev" + year[2];
			int cyclistInjuredKilled = 	  	  Integer.parseInt(tokens[7])
											+ Integer.parseInt(tokens[8]);
			context.write(new Text(key7), new IntWritable(cyclistInjuredKilled));
			
			//8. Year in which maximum Number Of Motorist Injured and Killed (combined)
			String key8 = "eig" + year[2];
			int motoristsInjuredKilled = 	  Integer.parseInt(tokens[9])
											+ Integer.parseInt(tokens[10]);
			context.write(new Text(key8), new IntWritable(motoristsInjuredKilled));
			

		}
	
	}
		
}


class ReducerTask extends Reducer <Text, IntWritable, Text, IntWritable>
{
    IntWritable value1   = new IntWritable();
		IntWritable value2   = new IntWritable();
		IntWritable value3   = new IntWritable();
		IntWritable value4   = new IntWritable();
		IntWritable value5   = new IntWritable();
		IntWritable value6   = new IntWritable();
		IntWritable value7   = new IntWritable();
		IntWritable value8   = new IntWritable();
		
		Text key1 = new Text();
		Text key2 = new Text();
		Text key3 = new Text();
		Text key4 = new Text();
		Text key5 = new Text();
		Text key6 = new Text();
		Text key7 = new Text();
		Text key8 = new Text();
		int v1=0,v2=0,v3=0,v4=0,v5=0,v6=0,v7=0,v8=0;
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		
		String s = key.toString();
		int sumVal = 0;
		
		for (IntWritable val : values)
		{
			sumVal+=val.get();	
		}
		if(s.substring(0,3).equals("one") && v1<sumVal){
		    v1=sumVal;
		    key1.set(s);
		    value1.set(sumVal);            
		}
		else if(s.substring(0,3).equals("two") && v2<sumVal){
		    v2=sumVal;
		    key2.set(s);
		    value2.set(sumVal);            
		}
		else if(s.substring(0,3).equals("thr") && v3<sumVal){
		    v3=sumVal;
		    key3.set(s);
		    value3.set(sumVal);            
		}
		else if(s.substring(0,3).equals("fou") && v4<sumVal){
		    v4=sumVal;
		    key4.set(s);
		    value4.set(sumVal);            
		}
		else if(s.substring(0,3).equals("fiv") && v5<sumVal){
		    v5=sumVal;
		    key5.set(s);
		    value5.set(sumVal);            
		}
		else if(s.substring(0,3).equals("six") && v6<sumVal){
		    v6=sumVal;
		    key6.set(s);
		    value6.set(sumVal);            
		}
		else if(s.substring(0,3).equals("sev") && v7<sumVal){
		    v7=sumVal;
		    key7.set(s);
		    value7.set(sumVal);            
		}
		else if(s.substring(0,3).equals("eig") && v8<sumVal){
		    v8=sumVal;
		    key8.set(s);
		    value8.set(sumVal);            
		}
		
	}
	@Override
	  protected void cleanup(Context context) throws IOException, InterruptedException {
	      context.write(key1,value1);
	      context.write(key2,value2);
	      context.write(key3,value3);
	      context.write(key4,value4);
	      context.write(key5,value5);
	      context.write(key6,value6);
	      context.write(key7,value7);
	      context.write(key8,value8);
	  }
}
