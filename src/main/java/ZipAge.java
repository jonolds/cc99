import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ZipAge implements Writable, WritableComparable<ZipAge> {
	private IntWritable zip, age;

	public ZipAge(IntWritable zip, IntWritable age) { this.zip = zip; this.age = age; }
	public ZipAge(int zip, int age) { this(new IntWritable(zip),new IntWritable(age)); }
	public ZipAge() { this.zip = new IntWritable(); this.age = new IntWritable(); }

	public void write(DataOutput out) throws IOException {
		zip.write(out);
		age.write(out);
	}
	
	public static ZipAge read(DataInput in) throws IOException {
		ZipAge zipPair = new ZipAge();
		zipPair.readFields(in);
		return zipPair;
	}

	public void readFields(DataInput in) throws IOException {
		zip.readFields(in);
		age.readFields(in);
	}

	public int compareTo(ZipAge other) {						// A compareTo B
		int returnVal = this.zip.compareTo(other.getZip());		// return -1: A < B
		if(returnVal != 0)										// return 0: A = B
			return returnVal;									// return 1: A > B
		if(this.age.toString().equals("*"))
			return -1;
		else if(other.getAge().toString().equals("*"))
			return 1;
		return this.age.compareTo(other.getAge());
	}
	
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ZipAge zipPair = (ZipAge) o;
		if (age != null ? !age.equals(zipPair.age) : zipPair.age != null) return false;
		if (zip != null ? !zip.equals(zipPair.zip) : zipPair.zip != null) return false;
		return true;
	}

	public int hashCode() {
		int result = (zip != null) ? zip.hashCode() : 0;
		return 163 * result + ((age != null) ? age.hashCode() : 0);
	}
	public String toString() { return "{zip=["+zip+"]"+ " neighbor=["+age+"]}"; }
	public ZipAge set(int w, int f) { this.zip.set(w); this.age.set(f); return this;}
	public void setZip(int zip){ this.zip.set(zip); }
	public void setAge(int age){ this.age.set(age); }
	public IntWritable getZip() { return zip; }
	public IntWritable getAge() { return age; }
}