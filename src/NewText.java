import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;

public class NewText {
	private Text value;

	public NewText(Text txt) { this.value = txt; }
	public NewText(String txt) { this(new Text(txt)); }
	public NewText() { this.value = new Text(); }

	public static NewText read(DataInput in) throws IOException {
		NewText txtPair = new NewText();
		txtPair.readFields(in);
		return txtPair;
	}

	public void write(DataOutput out) throws IOException { value.write(out); }
	public void readFields(DataInput in) throws IOException { value.readFields(in); }

	public String toString() { return "{txt=[" + value + "]"; }

	public int compareTo(NewText o) { return value.compareTo(o.getWord()); }
	public boolean equals(Object o) { return value.equals(o); }

	public int hashCode() { return 163 * (value != null ? value.hashCode() : 0); }
	public void set(String txt){ this.value.set(txt); }
	public Text getWord() { return value; }
}