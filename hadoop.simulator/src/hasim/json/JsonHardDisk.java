package hasim.json;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

class MapH extends HashMap<String, JsonHardDisk>{
	
}
public class JsonHardDisk {
	private double write;
	private double capacity;
	private double read;
	private double seekTime;
	
	public double getSeekTime() {
		return seekTime;
	}
	public void setSeekTime(double seekTime) {
		this.seekTime = seekTime;
	}
	public double getRead() {
		return read;
	}
	public void setRead(double read) {
		this.read = read;
	}
	public double getWrite() {
		return write;
	}
	public void setWrite(double write) {
		this.write = write;
	}
	public double getCapacity() {
		return capacity;
	}
	public void setCapacity(double capacity) {
		this.capacity = capacity;
	}
	
	@Override
	public String toString() {
		return "capacity:"+capacity+", read:"+ read+", write:"+ write+" ,seekTime:"+seekTime;
	}

	final static ObjectMapper mapper=new ObjectMapper();
	static Map<String, JsonHardDisk> maph;
	static {
		try {
			maph=mapper.readValue(new File("data/json/hardDisk.json"),
					new TypeReference<Map<String, JsonHardDisk>>() {});
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception{
//		H h = mapper.readValue(new File("data/json/h.json"), H.class);
//		System.out.println(h.toString());
		
		
		System.out.println(maph.toString());
		
		mapper.writeValue(new File("data/json/out.txt"), maph);
	}
	public JsonHardDisk copy() {
		JsonHardDisk result=new JsonHardDisk();
		result.capacity=this.capacity;
		result.seekTime=this.seekTime;
		result.read=this.read;
		result.write=this.write;
		return result;
	}
}
