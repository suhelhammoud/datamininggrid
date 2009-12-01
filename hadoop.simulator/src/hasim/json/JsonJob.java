package hasim.json;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


import dfs.HSplit;

public class JsonJob {
	private String jobName;
	private int numberOfMappers,numberOfReducers;
	private JsonSplit data;
	public JsonSplit getData() {
		return data;
	}
	public void setData(JsonSplit datat) {
		this.data = datat;
	}

	private List<JsonSplit> inputSplits= new ArrayList<JsonSplit>();
	private List<JsonSplit> outputSplits= new ArrayList<JsonSplit>();
	
	private String algorithm;
	private boolean useCombiner,useCompression;
	
	
	public String getJobName() {
		return jobName;
	}
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public int getNumberOfMappers() {
		return numberOfMappers;
	}
	public void setNumberOfMappers(int numberOfMappers) {
		this.numberOfMappers = numberOfMappers;
	}
	public int getNumberOfReducers() {
		return numberOfReducers;
	}
	public void setNumberOfReducers(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}
	public List<JsonSplit> getInputSplits() {
		return inputSplits;
	}
	public void setInputSplits(List<JsonSplit> inputSplits) {
		this.inputSplits = inputSplits;
	}
	public List<JsonSplit> getOutputSplits() {
		return outputSplits;
	}
	public void setOutputSplits(List<JsonSplit> outputSplits) {
		this.outputSplits = outputSplits;
	}
	public boolean isUseCombiner() {
		return useCombiner;
	}
	public void setUseCombiner(boolean useCombiner) {
		this.useCombiner = useCombiner;
	}
	public boolean isUseCompression() {
		return useCompression;
	}
	public void setUseCompression(boolean useCompression) {
		this.useCompression = useCompression;
	}
	
	
	@Override
	public String toString() {
		StringBuffer sb=new StringBuffer(jobName);
		sb.append(", algorithm:"+algorithm);
		sb.append("\nsplits:"+ inputSplits);
		return sb.toString();
	}
	
	public String getAlgorithm() {
		return algorithm;
	}
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper=new ObjectMapper();
		
		JsonJob job=mapper.readValue(new File("data/json/job.json"),
				JsonJob.class);
		
		mapper.writeValue(new File("data/json/out/job_out.txt"), job);

		System.out.println(job);

	}
	
	private void test(String name){
		try {
		
			Set<Integer> set=(Set<Integer>) Class.forName(name).newInstance();
			System.out.println(set);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
