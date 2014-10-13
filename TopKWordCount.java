import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


// Based on Exercise 1 -- Fixed-Length Word Count 
       
public class TopKWordCount {
        
 	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    	private final static IntWritable one = new IntWritable(1);
   	 	private Text word = new Text();
   	 	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            if (word.toString().length() == 7) {
                context.write(word, one);
            }
        }
    }
 } 


	// Reference from stackoverflow: http://stackoverflow.com/questions/13609366/hadoop-mapreduce-optimizing-top-n-word-count-mapreduce-job 

 	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	// Store the previous searched results
	TreeMap<Integer, String> mymap = new TreeMap<>();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int total = 0;
			for (IntWritable v : values) {
				total += v.get();
			}
			mymap.put(total, key.toString());
			if(mymap.size() > 100){ // Remove less frequent words
				mymap.remove(mymap.firstKey());
			}
		} 

		// Emit the results in te cleanup stage
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			Text key = new Text();
			for (Entry<Integer, String> entry : mymap.descendingMap().entrySet()) {
				key.set(entry.getValue());
				context.write(key ,new IntWritable(entry.getKey()));
			}
		}
	}

        
 public static void main(String[] args) throws Exception {
 	Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(TopKWordCount.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}