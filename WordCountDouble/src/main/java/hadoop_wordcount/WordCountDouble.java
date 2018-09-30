package hadoop_wordcount;

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

public class WordCountDouble 
{   //Map class to emit a key, value pair
	public static class DoubleWcMap extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		Text outkey = new Text(); //Key of the key, value pair that will be emitted
		IntWritable outval = new IntWritable(); //Value of the key, value pair that will be emitted

		//Logic to map double word count
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException 
		{
			String[] text = values.toString().split(" ");
			for (int i = 0; i < (text.length) - 1; i++) 
			{
				outkey.set(text[i] + "," + text[i + 1]);
				outval.set(1);
				context.write(outkey, outval);
			}
		}
	}
	//Reducer class
	public static class DoubleWcReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		IntWritable outval = new IntWritable(); //variable to store and write the count of each double word sequence
		
		//Reduce method to sum up the values, which are the occurrence counts for each key
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable target_seq : values) 
				sum = sum + target_seq.get();
			
			outval.set(sum);
			context.write(key, outval);
		}
	}
	
	//Main method to take command line arguments and create jobs
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "double program");
		job.setJarByClass(WordCountDouble.class);
		job.setMapperClass(DoubleWcMap.class);
		job.setReducerClass(DoubleWcReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
