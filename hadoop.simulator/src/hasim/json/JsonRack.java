package hasim.json;

import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class JsonRack {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(JsonRack.class);

	public List<List<String>> getMachines() {
		return machines;
	}
	public void setMachines(List<List<String>> machines) {
		this.machines = machines;
	}

	private String router;
	private List<List<String>> machines;
	public String getRouter() {
		return router;
	}
	public void setRouter(String router) {
		this.router = router;
	}
	

	@Override
	public String toString() {
		String result= "router:"+router+" , machines:";
		for (List<String> m : machines) {
			result+="\n"+m;
		}
		return result;
	}

	public static void main(String[] args) throws Exception{
		
		logger.info(JsonRealRack.readFile("data/json/rack_working.json"));
		
		if(true)return;
		
		ObjectMapper mapper=new ObjectMapper();
		
		Map<String, JsonMachine> machineMap=
			mapper.readValue(new File("data/json/machine.json"),
					new TypeReference<Map<String,JsonMachine>>() {});

		Map<String, JsonRack> m=
			mapper.readValue(new File("data/json/rack.json"),
					new TypeReference<Map<String,JsonRack>>() {});
		logger.info(m);

		mapper.writeValue(new File("data/json/out/rack_out.txt"), m);
		
		
		Map<String, JsonRealRack> outMap=new LinkedHashMap<String, JsonRealRack>();
		
		for (Map.Entry<String, JsonRack> e : m.entrySet()) {
			List<JsonMachine> jsonMachines=new ArrayList<JsonMachine>();

			for (List<String> lM : e.getValue().machines) {
				String machineName=lM.get(0);
				String machineType=lM.get(1);
				
				JsonMachine machine=machineMap.get(machineType).copy(machineName);
				jsonMachines.add(machine);
			}
			JsonRealRack realRack=new JsonRealRack();
			realRack.setRouter(e.getValue().router);
			realRack.setMachines(jsonMachines);
			
			outMap.put(e.getKey(), realRack);
			
			
		}
		
		mapper.writeValue(new File("data/json/out/realRack_out.txt"), outMap);

		
	}
}


