import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.stream.StreamSupport;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("unused")
public class Play {
	
	public static class TokenizerMapper extends Mapper<Object, Text, TextPair, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private TextPair word = new TextPair();
		private TextPair count = new TextPair("zzzzz", "zzzzz");
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.setA(itr.nextToken());
				word.setB(word.getA());
				context.write(word, one);
			}
			context.write(count, one);	
		}
	}
	
	public static class IntSumReducer extends Reducer<TextPair, IntWritable, TextPair, IntPair> {
		private IntPair result = new IntPair();
		
		public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();
			result.set(sum, sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = init(args);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	static Job init(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		FileUtils.deleteDirectory(new File(otherArgs[1]));
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Play.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
 
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);
 
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntPair.class);
		return job;
	}
}