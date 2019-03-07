import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WPairPriority implements Writable, WritableComparable<WPairPriority> {
	private Text word, filename;
	IntWritable priority;

	public WPairPriority(Text w, Text f) { this(); this.word.set(w); this.filename.set(f); }
	public WPairPriority(String w, String f) { this(); this.word.set(w); this.filename.set(f); }
	public WPairPriority(String w, String f, int p) { this(); this.setWord(w); this.setFilename(f); this.priority.set(p);}
	public WPairPriority() { this.word = new Text(); this.filename = new Text(); this.priority = new IntWritable(0); }

	public void write(DataOutput out) throws IOException {
		word.write(out);
		filename.write(out);
		priority.write(out);
	}
	
	public static WPairPriority read(DataInput in) throws IOException {
		WPairPriority pair = new WPairPriority();
		pair.readFields(in);
		return pair;
	}

	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		filename.readFields(in);
		priority.readFields(in);
	}

	public String toString() { return "{word=["+word+"]"+ " filename=["+filename+"]}"; }

	public int compareTo(WPairPriority o) {						// A compareTo B
		int returnVal = this.priority.compareTo(o.priority);
		if(returnVal != 0)
			return returnVal;
		returnVal = this.word.compareTo(o.getWord());		// return -1: A < B
		if(returnVal != 0)										// return 0: A = B
			return returnVal;									// return 1: A > B
		return this.filename.compareTo(o.getFilename());
	}
	
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		WPairPriority other = (WPairPriority) o;
		if (priority != null ? !priority.equals(other.priority) : other.priority != null) return false;

		if (word != null ? !word.equals(other.word) : other.word != null) return false;
		if (filename != null ? !filename.equals(other.filename) : other.filename != null) return false;

		return true;
	}

	public int hashCode() {
		int result = (word != null) ? word.hashCode() : 0;
		return 163 * result + ((filename != null) ? filename.hashCode() : 0);
	}

	public WPairPriority set(String w, String f) {this.word.set(w); this.filename.set(f); return this; }
	public WPairPriority setWord(String w){ this.word.set(w); return this; }
	public WPairPriority setFilename(String f){ this.filename.set(f); return this; }
	public WPairPriority setPriority(int p) {this.priority.set(p); return this; }
	public Text getWord() { return word; }
	public Text getFilename() { return filename; }
}