//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
//
//public class PairWritable<T> implements WritableComparable<T> {
//
//	public T a;
//	public T b;
//	
//	public PairWritable(){ this.a = null; this.b = null; }
//	
//	public PairWritable(T a, T b) { this.a = a; this.b = b; }
//	
//	public T getA() { return a; }
//	public String getAString() { return a.toString(); }
//	public T getB() { return b; }
//	public String getBString() { return b.toString(); }
//	
//	public void setA(T a) { this.a = a; }
//	public void setB(T b) { this.b = b; }
//	public void set(T a, T b){ this.a = a; this.b = b; }
//	
//	public int hashCode() { return a.hashCode()*163 + b.hashCode(); }
//	public String toString() { return a.toString() + "\t" + b.toString(); }
//	
//	public void readFields(DataInput in) throws IOException { a.readFields(in); b.readFields(in); }
//	public void write(DataOutput out) throws IOException { out.write(a.toString().getBytes()); out.write(b.toString().getBytes());; }
//	
//	public int compareTo(T tp) {
//		if
//	    return a.compareTo(tp.getA()) != 0 ? a.compareTo(tp.getA()) : b.compareTo(tp.getB());
//	}
//	public boolean equals(Object obj) {
//	    return (obj instanceof TextPair) ? a.equals(((TextPair)obj).getA()) && b.equals(((TextPair)obj).getB()) : false;
//	}
//
//}
