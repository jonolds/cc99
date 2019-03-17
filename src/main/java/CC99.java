import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Input is a map in adjacency list format, and performs a breadth-first search. The input format is:
 * "ID	 EDGES|DISTANCE|COLOR"
 * 		ID = the unique identifier for a node (assumed to be an int here)
 * 		EDGES = the list of edges emanating from the node (e.g. 3,8,9,12)
 * 		DISTANCE = the to be determined distance of the node from the source
 * 		COLOR = a simple status tracking field to keep track of when we're finished with a node
 * Source should have distance 0 and be GRAY. Others have distance Integer.MAX_VALUE and color WHITE.*/
public class CC99 extends Configured implements Tool {
	int iters = 7;
	/** WHITE or BLACK nodes are emitted as is. For every edge of a Color.GRAY node, we emit a new Node
	 * with distance incremented by one. The Color.GRAY node is then colored black and is also emitted. */
	public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			Node node = new Node(value.toString());

			// For each GRAY node, emit each of the edges as a new node (also GRAY)
			if (node.getColor() == Node.Color.GRAY) {
				for (int edge : node.getEdges()) {
					Node vnode = new Node(edge);
					vnode.setCost(node.getCost() + 1);
					vnode.setColor(Node.Color.GRAY);
					output.collect(new IntWritable(vnode.getId()), vnode.getLine());
				}
				node.setColor(Node.Color.BLACK);
			}
			output.collect(new IntWritable(node.getId()), node.getLine());
		}
	}

	/** A reducer class that just emits the sum of the input values. */
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		/** Make a new node which combines all information for this single node id. The Node should have
		 * - 1)The full list of edges. 2)The minimum distance. 3)The darkest Color. */
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			List<Integer> edges = null;
			int distance = Integer.MAX_VALUE;
			Node.Color color = Node.Color.WHITE;
			
			List<String> vals = new ArrayList<>();
			while(values.hasNext())
				vals.add(values.next().toString());
			println(key + "  " + vals.stream().map(x->x).collect(Collectors.joining("\t\t")));

			for(String value: vals) {
				Node u = new Node(key.get() + "\t" + value);
				// One one copy of the node will be the fully expanded version, which includes the edges
				if(u.getEdges().size() > 0)
					edges = u.getEdges();
				// Save the minimum distance
				if(u.getCost() < distance)
					distance = u.getCost();
				// Save the darkest color
				if(u.getColor().ordinal() > color.ordinal())
					color = u.getColor();
			}

			Node n = new Node(key.get());
			n.setCost(distance);
			n.setEdges(edges);
			n.setColor(color);
			output.collect(key, new Text(n.getLine()));
		}
	}

	/** The main driver for word count map/reduce program. Invoke this method to submit the map/reduce job.
	     @throws IOException When there is communication problems with the job tracker. */
	public int run(String[] args) throws Exception {
		int iterCount = 0;
		while (iterCount < iters) {
			JobConf conf = getJobConf(args);
			String input = (iterCount == 0) ? "input" : "output-graph-" + iterCount;
			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path("output-graph-" + (iterCount + 1)));
			JobClient.runJob(conf);
			iterCount++;
		}
		return 0;
	}
	
	private JobConf getJobConf(String[] args) {
		JobConf conf = new JobConf(getConf(), CC99.class);
		conf.setJobName("graphsearch");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);

		for (int i = 0; i < args.length; ++i) {
			if ("-m".equals(args[i]))
				conf.setNumMapTasks(Integer.parseInt(args[++i]));
			else if ("-r".equals(args[i]))
				conf.setNumReduceTasks(Integer.parseInt(args[++i]));
		}
		return conf;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		deleteOutputFolders(conf);
		int res = ToolRunner.run(conf, new CC99(), args);
		combineOutputFolders(conf);
		System.exit(res);
	}
	
	public static void deleteOutputFolders(Configuration conf) throws IllegalArgumentException, IOException {
		List<String> folders = Files.list(Paths.get("")).map(x->x.toString()).filter(x->x.startsWith("output")).collect(Collectors.toList());
		for(String folder : folders)
			new Path(folder).getFileSystem(conf).delete(new Path(folder), true);
	}	
	public static void combineOutputFolders(Configuration conf) throws IOException {
		List<String> outDirs = Files.list(Paths.get("")).map(x->x.toString()).filter(x->x.startsWith("output-graph")).collect(Collectors.toList());
		new File("output").mkdir();
		for(String outDir : outDirs) {
			List<String> files = Files.list(Paths.get(outDir + "/")).map(x->x.toString()).filter(x->!(x.endsWith("crc")||x.endsWith("SUCCESS"))).collect(Collectors.toList());
			for(String file : files) {
				String newName = file.substring(13).replace('\\', '-');
				Files.copy(new File(file).toPath(), new File("output\\" + newName + ".txt").toPath());
			}
			new Path(outDir).getFileSystem(conf).delete(new Path(outDir), true);
		}
		String outStr = "input\n" + FileUtils.readFileToString(new File("input" + File.separator + "file0"), "UTF-8") + "\n\n";
		List<String> outFiles = Files.list(Paths.get("output")).map(x->x.toString()).collect(Collectors.toList());
		for(String file: outFiles)
			outStr += file + "\n" + FileUtils.readFileToString(new File(file), "UTF-8") + "\n";
		FileUtils.writeStringToFile(new File("output" + File.separator + "outputAll.txt"), outStr, "UTF-8");
		outFiles.forEach(x->FileUtils.deleteQuietly(new File(x)));
	}
	static <T>void println(T t) { System.out.println(t.toString()); }
}