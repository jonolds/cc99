import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {
	public IntWritable a, b;
	
	public IntPair(){ this.a = new IntWritable(); this.b = new IntWritable(); }
	public IntPair(IntWritable a, IntWritable b) { this.a = a; this.b = b; }
	public IntPair(int a, int b){ this.a = new IntWritable(a); this.b = new IntWritable(b); }
	
	public IntWritable getA() { return a; }
	public IntWritable getB() { return b; }
	public int a() { return a.get(); }
	public int b() {return b.get(); }
	
	public void setA(IntWritable a) { this.a = a; }
	public void setA(int a) { this.a = new IntWritable(a); }
	public void setB(IntWritable b) { this.b = b; }
	public void setB(int b) { this.b = new IntWritable(b); }
	public void set(int a, int b) { this.a= new IntWritable(a); this.b = new IntWritable(b); }
	public void set(IntWritable a, IntWritable b) { this.a = a; this.b = b; }
	
	public int hashCode() { return a.hashCode()*163 + b.hashCode(); }
	public String toString() { return a + "  " + b; }
	
	public void readFields(DataInput in) throws IOException { a.readFields(in); b.readFields(in); }
	public void write(DataOutput out) throws IOException { a.write(out); b.write(out); }
	
	public int compareTo(IntPair o) {
	    return a.compareTo(o.getA()) != 0 ? a.compareTo(o.getA()) : b.compareTo(o.getB());
	}
	public boolean equals(Object o) {
	    return (o instanceof IntPair) ? a.equals(((IntPair)o).getA()) && b.equals(((IntPair)o).getB()) : false;
	}
}