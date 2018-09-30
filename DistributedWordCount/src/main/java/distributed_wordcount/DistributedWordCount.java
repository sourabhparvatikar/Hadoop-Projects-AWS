package distributed_wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;
import java.net.URI;
import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistributedWordCount 
{	////Map class to emit a key, value pair
	public static class Map extends Mapper<Object, Text, Text, IntWritable> 
	{
		private final static IntWritable one = new IntWritable(1); //Value of the (key, value) pair that will be emitted
		private Text word = new Text(); //Key of the (key, value) pair that will be emitted

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer iterator = new StringTokenizer(value.toString()); //Tokenizing the input string
			String distributed_words = FileUtils.readFileToString(new File("./cachedFile")); //reading the input from cached file which will be available as local file in each instance.
			String[] distributed_word_list = distributed_words.split(" "); //converting cached file input to String list
			Set<String> distributedWordCount = new HashSet<String>(Arrays.asList(distributed_word_list)); //converting String list to Set

			while (iterator.hasMoreTokens()) 
			{
				word.set(iterator.nextToken());
				if (distributedWordCount.contains(word.toString())) //checking if cached set contains the word
					context.write(word, one);
			}
		}
	}
	
	//Reducer class
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		IntWritable outval = new IntWritable();
		
		//Reduce method to sum up the values, which are the occurrence counts for each key
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();
			
			outval.set(sum);
			context.write(key, outval);
		}
	}

	//Main method to take command line arguments and create jobs
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(DistributedWordCount.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.addCacheFile(new URI(args[2] + "#cachedFile")); // args[2] is the URI with s3: protocol. It will be accessible as a local file in ec2 instance called 'cachedFile'
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}
