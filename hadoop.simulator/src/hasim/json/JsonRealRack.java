package hasim.json;

import java.io.File;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonRealRack{
	private String name="rack 0";
	
	private String router;
	private List<JsonMachine> machines;
	public String getRouter() {
		return router;
	}
	public void setRouter(String router) {
		this.router = router;
	}
	public List<JsonMachine> getMachines() {
		return machines;
	}
	public void setMachines(List<JsonMachine> machines) {
		this.machines = machines;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	
	@Override
	public String toString() {
		return "router:"+router+"\n"+ machines;
	}
	
	public static JsonRealRack readFile(String fileName)throws Exception{
		ObjectMapper mapper=new ObjectMapper();
		JsonRealRack rack=
			mapper.readValue(new File(fileName), JsonRealRack.class);

		return rack;
	}
}