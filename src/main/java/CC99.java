import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CC99 extends MRHelp {
	public static class VoterMapper extends Mapper<Object, Text, ZipAge, Text> {
		ZipAge zipage = new ZipAge();
		Text name = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] voter = value.toString().split("\\s+");
			zipage.setZip(voter[4]);
			zipage.setAge(Integer.parseInt(voter[3]));
			name.set(voter[1] + " " + voter[2]);
			context.write(zipage, name);
			
		}
	}
	
	public static class ZipAgeReducer extends Reducer<ZipAge, Text, ZipAge, Text> {
		Text nameText = new Text();
		public void reduce(ZipAge key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			nameText.set(StreamSupport.stream(values.spliterator(), false).map(x->x.toString()).collect(Collectors.joining(" ")));
			context.write(key, nameText);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = initializeJob(args);
		job.setJarByClass(CC99.class);
		job.setMapperClass(VoterMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(ZipAgeReducer.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(ZipAge.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static Job initializeJob(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		new Path(args[1]).getFileSystem(conf).delete(new Path(otherArgs[1]), true);
		Job job = Job.getInstance(conf, "zip age");
		return job;
	}
}