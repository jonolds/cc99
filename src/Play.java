import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Play {
	static final String DELIM = "*-*-*";
	
	public static class TokenizerMapper extends Mapper<Object, TextX, TextX, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		public void map(Object key, TextX value, Context context) throws IOException, InterruptedException {
			context.write(new TextX(DELIM+"Word Count: "), one);
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens())
				context.write(new TextX(itr.nextToken()), one);
		}
	}
	
	public static class IntSumReducer extends Reducer<TextX, IntWritable, TextX, IntWritable> {
		private IntWritable result = new IntWritable();
		private MultipleOutputs<TextX, IntWritable> mos;
		
		public void setup(Context context) { mos = new MultipleOutputs<TextX, IntWritable>(context); 
			System.out.println("red cleaner");
		}
		
		public void reduce(TextX key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			if(key.toString().startsWith(DELIM)) {
				int mapcount = StreamSupport.stream(values.spliterator(), false).mapToInt(x->x.get()).sum();
				mos.write("mapCallCount", key.toString().substring(5), new IntWritable(mapcount));
			}
			
			else {
				int sum = 0;
				for (IntWritable val : values)
					sum += Integer.parseInt(val.toString());
				result.set(sum);

				context.write(key, result);
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException { mos.close(); }
	}
	
	public static void main(String[] args) throws Exception {
		Job job = init(args);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "mapCallCount", TextOutputFormat.class, Play.TextX.class, IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	static Job init(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		if(new File("output").exists())
			Files.walk(Paths.get("output")).sorted(Comparator.reverseOrder()).forEach(x->x.toFile().delete());
		
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Play.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Play.TextX.class);
		job.setOutputValueClass(IntWritable.class);
		return job;
	}
	
	static class TextX implements WritableComparable<TextX> {  
		public Text val;
		
		public TextX(String a) {
			this.val = new Text(a); 

		}
		public TextX() { this.val = new Text(); }
		
		public void set(Text t) { 
			this.val = t;
		}
		public void set(String str) {
			this.val.set(str);
		}
		
		public String toString() { 
			return this.val.toString();
		}
		
		public void readFields(DataInput in) throws IOException { 
			val.readFields(in);

		}
		public void write(DataOutput out) throws IOException { 
			val.write(out); 

		}
		
		public int compareTo(TextX o) {
			return val.compareTo(o.val);
		}
		public boolean equals(TextX o) {
		    return val.equals(o.val);
		}
	}
}
	
