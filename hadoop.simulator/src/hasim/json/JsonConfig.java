package hasim.json;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class JsonConfig  {

	
	private  int maxIM;
	public int getMaxIM() {
		return maxIM;
	}
	public void setMaxIM(int maxIM) {
		maxIM = maxIM;
	}

	
	private  double heartbeat;
	public double getHeartbeat() {
		return heartbeat;
	}
	public void setHeartbeat(double heartbeat) {
		this.heartbeat = heartbeat;
	}
	
	private  double propDelay;
	public  double getPropDelay() {
		return propDelay;
	}
	public  void setPropDelay(double propDelay) {
		this.propDelay = propDelay;
	}
	
	private double maxSplitSize;
	
	public double getMaxSplitSize() {
		return maxSplitSize;
	}
	public void setMaxSplitSize(double maxSplitSize) {
		this.maxSplitSize = maxSplitSize;
	}

	
	private double delta;

	public double getDelta() {
		return delta;
	}
	public void setDelta(double delta) {
		this.delta = delta;
	}


	private final static Logger logger = Logger.getLogger(JsonConfig.class);

	

	private Map<String, Object> map;
	

	
	public Map<String, Object> getMap() {
		return map;
	}
	public void setMap(Map<String, Object> map2) {
		map = map2;
	}
	
	



	@Override
	public String toString() {
		StringBuffer sb=new StringBuffer();
		sb.append("\n maxIM:"+ getMaxIM());
		sb.append("\n propDely:"+ getPropDelay());

		sb.append("\n hearbeat:"+ getHeartbeat());
		sb.append("\n maxSplitSize:"+ getMaxSplitSize());
		sb.append("\n delta:"+ getDelta());

		sb.append("\n map:"+ getMap());
		return sb.toString();
	}
	
	public static JsonConfig readFrom(String filename)throws Exception{
		if(! new File(filename).exists()){
			logger.error("file "+filename+" not found");
			throw new IOException();
		}
		ObjectMapper mapper=new ObjectMapper();

		JsonConfig result=
			mapper.readValue(new File(filename),
					JsonConfig.class);

		return result;
	}
	public static Object read(String filename, Class cls)throws Exception{
		if(! new File(filename).exists()){
			logger.error("file "+filename+" not found");
			throw new IOException();
		}
		ObjectMapper mapper=new ObjectMapper();

		Object result=
			mapper.readValue(new File(filename),
					cls);

		return result;
	}
	
	public static void main(String[] args) throws Exception {
		Object config1= JsonConfig.read("data/json/config.json", JsonConfig.class);
		logger.info(config1);
		JsonConfig config= JsonConfig.readFrom("data/json/config.json");
		//logger.info(config);
		//logger.info("maxIM:"+ config.getMaxIM());
		//List<Integer> list=(List<Integer>)config.getMap().get("someList");
//		logger.info("list:"+ list);
		
	}
}
