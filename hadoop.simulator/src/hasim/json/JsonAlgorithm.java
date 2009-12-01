package hasim.json;

import java.io.File;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class JsonAlgorithm {

	private double mapCost = 1.0;
	private double mapSize = 1.0;
	private double mapRecords = 1.0;
	
	private double combineCost = 1.0;
	private double combineSize = 1.0;
	private double combineRecords = 1.0;
	
	private double reduceCost = 1.0;
	private double reduceSize = 1.0;
	private double reduceRecords = 1.0;
	public double getMapCost() {
		return mapCost;
	}
	public void setMapCost(double mapCost) {
		this.mapCost = mapCost;
	}
	public double getMapSize() {
		return mapSize;
	}
	public void setMapSize(double mapSize) {
		this.mapSize = mapSize;
	}
	public double getMapRecords() {
		return mapRecords;
	}
	public void setMapRecords(double mapRecords) {
		this.mapRecords = mapRecords;
	}
	public double getCombineCost() {
		return combineCost;
	}
	public void setCombineCost(double combineCost) {
		this.combineCost = combineCost;
	}
	public double getCombineSize() {
		return combineSize;
	}
	public void setCombineSize(double combineSize) {
		this.combineSize = combineSize;
	}
	public double getCombineRecords() {
		return combineRecords;
	}
	public void setCombineRecords(double combineRecords) {
		this.combineRecords = combineRecords;
	}
	public double getReduceCost() {
		return reduceCost;
	}
	public void setReduceCost(double reduceCost) {
		this.reduceCost = reduceCost;
	}
	public double getReduceSize() {
		return reduceSize;
	}
	public void setReduceSize(double reduceSize) {
		this.reduceSize = reduceSize;
	}
	public double getReduceRecords() {
		return reduceRecords;
	}
	public void setReduceRecords(double reduceRecords) {
		this.reduceRecords = reduceRecords;
	}
	
	@Override
	public String toString() {
		return ""+mapCost;
	}
	
	public static void main(String[] args) throws Exception{
		ObjectMapper mapper=new ObjectMapper();
		Map<String, JsonAlgorithm> amap = mapper.readValue(new File("data/json/algorithm.json"), 
				new TypeReference<Map<String, JsonAlgorithm>>() {});
		
		System.out.println(amap.toString());
	}
}
