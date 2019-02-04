import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {
	public IntWritable a;
	public IntWritable b;
	
	public IntPair(){
	    this.a = new IntWritable();
	    this.b = new IntWritable();
	}
	
	public IntPair(IntWritable a, IntWritable b) {
	    this.a = a;
	    this.b = b;
	}
	public IntPair(int a, int b){
	    this.a = new IntWritable(a);
	    this.b = new IntWritable(b);
	}
	
	public IntWritable getA() {
	    return a;
	}
	
	
	public void setA(IntWritable a) {
	    this.a = a;
	}
	
	public IntWritable getB() {
	    return b;
	}
	
	public void setB(IntWritable b) {
	    this.b = b;
	}
	public void set(int a, int b){
	    this.a= new IntWritable(a);
	    this.b= new IntWritable(b);
	}
	
	@Override
	public int hashCode() {
	    return a.hashCode()*163+b.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
	    if(obj instanceof IntPair){
	        IntPair tp=(IntPair)obj;
	        return a.equals(tp.getA())&&b.equals(tp.getB());
	    }
	    return false;
	}
	
	@Override
	public String toString() {
	    return a+"  "+b;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
	    a.readFields(in);
	    b.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
	    a.write(out);
	    b.write(out);
	}
	
	
	@Override
	public int compareTo(IntPair o) {
	    return a.compareTo(o.getA()) != 0 ? a.compareTo(o.getA()) : b.compareTo(o.getB());
	}
	
	public IntPair reverse() {
		return new IntPair(b,a);
	}
}