package dfs;

import gridsim.GridSim;
import gridsim.Gridlet;
import gridsim.net.Link;
import hasim.Datum;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * split name and splitId must be unique
 * 
 * @author hadoop
 * 
 */
public class HSplit extends Datum{

	
	
	public HSplit(String name, double size, double delta, double entries) {
		super(name, size, delta, entries);
	}
	
	public HSplit(Datum data){
		super(data);
	}


	List<String> replica=new ArrayList<String>();
	String location;

	
	
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
		addLocation(location);
	}
	
	public boolean addLocation(String loc){
		if(replica.contains(loc))return false;
		return replica.add(loc);
	}
	public boolean removeLocation(String loc){
		if(!replica.contains(loc))return false;
		return replica.remove(loc);
	}

	public List<String> getReplica() {
		return replica;
	}

	public void setReplica(List<String> replica) {
		this.replica = replica;
	}
	
	
	
	


	
}
