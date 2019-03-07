import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FILECOUNT implements Writable, WritableComparable<FILECOUNT> {
	private Text filename;
	private IntWritable count;

	public FILECOUNT(Text filename, IntWritable count) { this.filename = filename; this.count = count; }
	public FILECOUNT(String filename, int count) { this(new Text(filename), new IntWritable(count)); }
	public FILECOUNT() { this.filename = new Text(); this.count = new IntWritable(); }

	public static FILECOUNT read(DataInput in) throws IOException {
		FILECOUNT filecount = new FILECOUNT();
		filecount.readFields(in);
		return filecount;
	}

	public void readFields(DataInput in) throws IOException {
		filename.readFields(in);
		count.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		filename.write(out);
		count.write(out);
	}

	public String toString() { return "filename=[" + filename + "]"+ " count=[" + count + "]"; }

	public int compareTo(FILECOUNT o) {						// A compareTo B
		return filename.compareTo(o.filename);
	}
	
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FILECOUNT wordfile = (FILECOUNT) o;

		if (filename != null ? !filename.equals(wordfile.filename) : wordfile.filename != null) return false;

		return true;
	}

	public int hashCode() {
		int result = (filename != null) ? filename.hashCode() : 0;
		return 163 * result + ((count != null) ? count.hashCode() : 0);
	}

	public FILECOUNT set(String filename, int count) { this.filename.set(filename); this.count.set(count); return this; }
	public FILECOUNT setCount(int count){ this.count.set(count); return this; }
	public FILECOUNT setFilename(String filename){ this.filename.set(filename); return this; }
	public IntWritable getCount() { return count; }
	public Text getFilename() { return filename; }
}