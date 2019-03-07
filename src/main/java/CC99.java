import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CC99 {
	
	
	public static class InvIdxMapper extends Mapper<LongWritable, Text, WORDFILE, IntWritable> {
		HashMap<String, Integer> THEMAP;
		static final String DELIM = "!-!-!";
		
		protected void setup(Context context) {
			THEMAP = new HashMap<String, Integer>();
		}
		
		protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			context.write(new WORDFILE(String.valueOf("AA"), filename), new IntWritable(1));
			
			for(String tok : line.toString().toLowerCase().split(" "))
				if(tok.length() > 0)
					THEMAP.put(tok+DELIM+filename, THEMAP.containsKey(tok+DELIM+filename) ? THEMAP.get(tok+DELIM+filename) + 1 : 1);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("!!!!!!!!!!!!!!!!!!!MAP CLEANUP    " + THEMAP.size());

			WORDFILE wordfile = new WORDFILE();
			IntWritable count = new IntWritable();
			Iterator<Entry<String, Integer>> it = THEMAP.entrySet().iterator();
			while(it.hasNext()) {
				Entry<String, Integer> mapElem = it.next();
				String[] key_parts = mapElem.getKey().split(DELIM);
				count.set(mapElem.getValue());
				context.write(wordfile.set(key_parts[0], key_parts[1]), count);
			}
		}
	}
	
	public static class WORDFILEPartitioner extends Partitioner<WORDFILE, IntWritable> {
		public int getPartition(WORDFILE key, IntWritable value, int numPartitions) {
			if(key.getWord().toString().contains("AA"))
				return 0;
			else
				return Math.abs(key.getWord().hashCode()) % numPartitions;
		}
	}
	
	public static class InvIdxReducer extends Reducer<WORDFILE, IntWritable, Text, Text> {
		String plist = "", lastword = null;
		
		protected void reduce(WORDFILE key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int valuesSum = StreamSupport.stream(values.spliterator(), false).mapToInt(x->x.get()).sum();
			
			if(key.getWord().toString().startsWith("AA"))
				context.write(new Text(key.getWord() + "__" + key.getFilename()), new Text(String.valueOf(valuesSum)));
			else {
				if(lastword != null	&& !lastword.equals(key.getWord().toString())) {
					context.write(new Text(lastword), new Text(plist));
					plist = "";
				}
				plist += ((plist.length() == 0) ? "":";") + key.getFilename() + ":" + valuesSum;
				lastword = key.getWord().toString();
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text(lastword), new Text(plist));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = initializeJob(args);
		job.setNumReduceTasks(3);
		job.setJarByClass(CC99.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//MAPPER
		job.setMapperClass(InvIdxMapper.class);
		job.setMapOutputKeyClass(WORDFILE.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//PARTITIONER
		job.setPartitionerClass(WORDFILEPartitioner.class);
		
		//REDUCER/OUTPUT
		job.setReducerClass(InvIdxReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.out.println(job.waitForCompletion(true) ? "Job SUCCESS" : "Job FAILED");

		System.exit(0);
	}
	
	public static Job initializeJob(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		new Path(args[1]).getFileSystem(conf).delete(new Path(otherArgs[1]), true);
		Job job = Job.getInstance(conf, "WordPair Count");
		return job;
	}
}