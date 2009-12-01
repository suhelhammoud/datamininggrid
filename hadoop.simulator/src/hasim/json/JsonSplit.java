package hasim.json;

import java.util.ArrayList;
import java.util.List;

public class JsonSplit {

	//	{"id":"01", "location":"m1", "size":1000.00, "records":1000},
	private String location,id;
	private double size,records;
	private List<String> replica=new ArrayList<String>();

	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public double getSize() {
		return size;
	}
	public void setSize(double size) {
		this.size = size;
	}
	public double getRecords() {
		return records;
	}
	public void setRecords(double records) {
		this.records = records;
	}
	

	public List<String> getReplica() {
		return replica;
	}
	public void setReplica(List<String> replica) {
		this.replica = replica;
	}
	@Override
	public String toString() {
		return "(id="+id+", location:"+location+", size"+size+
			" ,records:"+records+", replica:"+replica;
	}
}
