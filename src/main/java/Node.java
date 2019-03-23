import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;

enum Color { WHITE, GRAY, BLACK }

public class Node {
	private int id, cost;
	private List<Integer> edges = new ArrayList<Integer>();
	private Color color = Color.WHITE;

	public Node(String str) {
		String[] map = str.split("\t");
		String key = map[0], value = map[1];
		String[] tokens = value.split("\\|");

		//ID - EDGES - COST - COLOR
		this.id = Integer.parseInt(key);
		Arrays.stream(tokens[0].split(",")).filter(x->x.length()>0).forEach(x->edges.add(Integer.parseInt(x)));
		this.cost = (tokens[1].equals("Integer.MAX_VALUE")) ? Integer.MAX_VALUE : Integer.parseInt(tokens[1]);
		this.color = Color.valueOf(tokens[2]);
	}

	public Node(int id) { this.id = id; }
	public int getId() { return this.id; }

	public List<Integer> getEdges() { return this.edges; }
	public void setEdges(List<Integer> edges) { this.edges = edges; }
	
	public int getCost() { return this.cost; }
	public void setCost(int cost) { this.cost = cost; }

	public Color getColor() { return this.color; }
	public void setColor(Color color) { this.color = color; }

	public Text getLine() {
		StringBuffer s = new StringBuffer(edges.stream().map(x->x.toString()).collect(Collectors.joining(",")) + "|");
		s.append(cost < Integer.MAX_VALUE ? cost : "Integer.MAX_VALUE").append("|");
		s.append(color.toString());
		return new Text(s.toString());
	}
}