import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

class BigramWritable implements WritableComparable<BigramWritable> {
	private String a, b;
	
	BigramWritable() { 
		this.a = null;
		this.b = null;
	}
	BigramWritable(String a, String b) { set(a, b); }
	
	public void setA(String s) { this.a = s;}
	public void setB(String s) { this.b = s;}
	public void set(String s0, String s1) { 
		this.a = s0;
		this.b = s1;
	}
	public String getA() { return a; }
	public String getB() { return b; }
	
	public void write(DataOutput out) throws IOException {
		out.writeChars(a);
		out.writeChars(b);
	}

	public void readFields(DataInput in) throws IOException {
		a = in.readLine();
		b = in.readLine();
	}

	public int compareTo(BigramWritable c) {
		if(this.a.compareTo(c.getA()) != 0)
			return this.a.compareTo(c.getA());
		else {
			if(this.b.compareTo(c.getB()) != 0)
				return this.b.compareTo(c.getB());
			else
				return 0;
		}
	}
}