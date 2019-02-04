import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class TextPair implements WritableComparable<TextPair>{  
	public Text a;
	public Text b;
	
	public TextPair(){ this.a = new Text(); this.b = new Text(); }
	
	public TextPair(Text a, Text b) { this.a = a; this.b = b; }
	public TextPair(String a,String b){ this.a = new Text(a); this.b = new Text(b); }
	
	public Text getA() { return a; }
	public String getAString() { return a.toString(); }
	public Text getB() { return b; }
	public String getBString() { return b.toString(); }
	
	public void setA(Text a) { this.a = a; }
	public void setA(String a) { this.a = new Text(a); }
	public void setB(Text b) { this.b = b; }
	public void setB(String b) { this.b = new Text(b); }
	public void set(Text a,Text b){ this.a = a; this.b = b; }
	public void set(String a, String b) { this.a = new Text(a); this.b = new Text(b); }
	
	public int hashCode() { return a.hashCode()*163 + b.hashCode(); }
	public String toString() { return a + "\t" + b; }
	public TextPair reverse() { return new TextPair(b, a); }
	
	public void readFields(DataInput in) throws IOException { a.readFields(in); b.readFields(in); }
	public void write(DataOutput out) throws IOException { a.write(out); b.write(out); }
	
	public int compareTo(TextPair tp) {
	    return a.compareTo(tp.getA()) != 0 ? a.compareTo(tp.getA()) : b.compareTo(tp.getB());
	}
	public boolean equals(Object obj) {
	    return (obj instanceof TextPair) ? a.equals(((TextPair)obj).getA()) && b.equals(((TextPair)obj).getB()) : false;
	}
}