import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WORDPAIR implements Writable, WritableComparable<WORDPAIR> {
	private Text word, filename;

	public WORDPAIR(Text word, Text filename) { this.word = word; this.filename = filename; }
	public WORDPAIR(String word, String filename) { this(new Text(word),new Text(filename)); }
	public WORDPAIR() { this.word = new Text(); this.filename = new Text(); }

	public void write(DataOutput out) throws IOException {
		word.write(out);
		filename.write(out);
	}
	
	public static WORDPAIR read(DataInput in) throws IOException {
		WORDPAIR wordPair = new WORDPAIR();
		wordPair.readFields(in);
		return wordPair;
	}

	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		filename.readFields(in);
	}

	public int compareTo(WORDPAIR other) {						// A compareTo B
		int returnVal = this.word.compareTo(other.getWord());	// return -1: A < B
		if(returnVal != 0)										// return 0: A = B
			return returnVal;									// return 1: A > B
		if(this.filename.toString().equals("*"))
			return -1;
		else if(other.getFilename().toString().equals("*"))
			return 1;
		return this.filename.compareTo(other.getFilename());
	}
	
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		WORDPAIR wordPair = (WORDPAIR) o;
		if (filename != null ? !filename.equals(wordPair.filename) : wordPair.filename != null) return false;
		if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;
		return true;
	}

	public int hashCode() {
		int result = (word != null) ? word.hashCode() : 0;
		return 163 * result + ((filename != null) ? filename.hashCode() : 0);
	}
	public String toString() { return "{word=["+word+"]"+ " neighbor=["+filename+"]}"; }
	public WORDPAIR set(String w, String f) { this.word.set(w); this.filename.set(f); return this;}
	public void setWord(String word){ this.word.set(word); }
	public void setFilename(String filename){ this.filename.set(filename); }
	public Text getWord() { return word; }
	public Text getFilename() { return filename; }
}