import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class PairText implements WritableComparable<PairText>{  
	public Text a, b;
	
	public PairText(){ this.a = new Text(); this.b = new Text(); }
	
	public PairText(Text a, Text b) { this.a = a; this.b = b; }
	public PairText(String a,String b){ this.a = new Text(a); this.b = new Text(b); }
	public PairText(String a) { this.a = new Text(a); this.b = new Text("");}
	
	public Text getA() { return a; }
	public Text getB() { return b; }
	public String a() { return a.toString(); }
	public String b() { return b.toString(); }
	
	public void setA(Text a) { this.a = a; }
	public void setA(String a) { this.a = new Text(a); }
	public void setB(Text b) { this.b = b; }
	public void setB(String b) { this.b = new Text(b); }
	public void set(Text a,Text b){ this.a = a; this.b = b; }
	public void set(String a, String b) { this.a = new Text(a); this.b = new Text(b); }
	
	public int hashCode() { return a.hashCode()*163 + b.hashCode(); }
	public String toString() { return a + "  " + b; }
	
	public void readFields(DataInput in) throws IOException { a.readFields(in); b.readFields(in); }
	public void write(DataOutput out) throws IOException { a.write(out); b.write(out); }
	
	public int compareTo(PairText o) {
	    return a.compareTo(o.getA()) != 0 ? a.compareTo(o.getA()) : b.compareTo(o.getB());
	}
	public boolean equals(Object o) {
	    return (o instanceof PairText) ? a.equals(((PairText)o).getA()) && b.equals(((PairText)o).getB()) : false;
	}
}