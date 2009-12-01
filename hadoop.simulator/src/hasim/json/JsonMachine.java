package hasim.json;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class JsonMachine {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(JsonMachine.class);

	
	
	
	private String hostName;
	private double baudRate;
	private double virtualMemory;
	
	private JsonHardDisk hardDisk;
	private JsonCpu cpu;
	
	
	private  int maxMapper,maxReducer;
	private double mapperVirtualMemory,reducerVirtualMemory;
	
	
	public int getMaxMapper() {
		return maxMapper;
	}
	public void setMaxMapper(int maxMapper) {
		this.maxMapper = maxMapper;
	}
	public int getMaxReducer() {
		return maxReducer;
	}
	public void setMaxReducer(int maxReducer) {
		this.maxReducer = maxReducer;
	}
	public double getMapperVirtualMemory() {
		return mapperVirtualMemory;
	}
	public void setMapperVirtualMemory(double mapperVirtualMemory) {
		this.mapperVirtualMemory = mapperVirtualMemory;
	}
	public double getReducerVirtualMemory() {
		return reducerVirtualMemory;
	}
	public void setReducerVirtualMemory(double reducerVirtualMemory) {
		this.reducerVirtualMemory = reducerVirtualMemory;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public double getBaudRate() {
		return baudRate;
	}
	public void setBaudRate(double baudRate) {
		this.baudRate = baudRate;
	}
	public double getVirtualMemory() {
		return virtualMemory;
	}
	public void setVirtualMemory(double virtualMemory) {
		this.virtualMemory = virtualMemory;
	}
	
	
	public JsonHardDisk getHardDisk() {
		return hardDisk;
	}
	public void setHardDisk(JsonHardDisk hardDisk) {
		this.hardDisk = hardDisk;
	}
	public JsonCpu getCpu() {
		return cpu;
	}
	public void setCpu(JsonCpu cpu) {
		this.cpu = cpu;
	}
	
	
	public JsonMachine copy(String name){
		JsonMachine result=new JsonMachine();
		result.hostName=name;
		result.baudRate=baudRate;
		result.virtualMemory=virtualMemory;
		result.cpu=cpu.copy();
		result.hardDisk=hardDisk.copy();
		return result;
	}
	
	@Override
	public String toString() {
		StringBuffer sb=new StringBuffer();
		//sb.append("{\nhostName:"+hostName);
		sb.append(",\nbaudRate:"+baudRate);
		sb.append(",\nvirtualMemeory:"+virtualMemory);
		sb.append(",\ncpu:"+cpu);
		sb.append(",\nhardDisk:"+hardDisk);

		return sb.toString();
	}
	
	public static void main(String[] args) throws Exception{
		ObjectMapper mapper=new ObjectMapper();
		
		Map<String, JsonMachine> m=
			mapper.readValue(new File("data/json/machine.json"),
					new TypeReference<Map<String, JsonMachine>>() {});
		logger.info(m);
		
		mapper.writeValue(new File("data/json/machine_out.txt"), m);

	}
}
