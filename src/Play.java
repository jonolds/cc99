import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.stream.StreamSupport;

import org.apache.commons.io.FileUtils;
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
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("unused")
public class Play {
	
	public static class TokenizerMapper extends Mapper<Object, JonText, JonText, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private JonText word = new JonText();

		public void map(Object key, JonText value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<JonText, IntWritable, JonText, IntWritable> {
		private IntWritable result = new IntWritable();
				
		public void reduce(JonText key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = StreamSupport.stream(values.spliterator(), false).mapToInt(x->x.get()).sum();
			result.set(sum);
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
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(JonText.class);
		job.setOutputValueClass(IntWritable.class);
		return job;
	}
}

class JonText implements WritableComparable<JonText> {
	private String s;
	
	JonText() { this.s = null; }
	JonText(String str) { this.s = str; }
	
	void set(String str) { this.s = str; }
	String get() { return s; }

	public void write(DataOutput out) throws IOException {
		out.writeChars(s);
	}
	
	public void readFields(DataInput in) throws IOException {
		s = in.readLine();
	}

	public int compareTo(JonText o) {
		return s.compareTo(o.get());
	}

}