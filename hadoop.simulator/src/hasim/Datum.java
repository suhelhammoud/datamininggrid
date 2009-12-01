package hasim;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.sql.rowset.Predicate;


import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_predicate;
import eduni.simjava.Sim_type_p;


public class Datum{
	
	
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Datum.class);



	private static int totalId=HTAG.values().length+ HTAG.HDBASE+1000;

	final private int id;
	final protected String name;
	final protected double size;
	final protected double delta;

	protected double ratio = 0.25;
	protected double records;

	protected double currentSize=0;	
	
	public double tmpDelta=0.25;
	
	public LinkedList<Double> deltaList=new LinkedList<Double>();
	
	public HTAG tag;
	public int returnTag;
	
	public Object data;


	public String getName() {
		return name;
	}
	public int id(){
		return this.id;
	}

	public Set<Sim_entity> getBlockingUsers() {
		return blockingUsers;
	}
	public boolean registerBlocking(Sim_entity user, int returnTag){
		this.returnTag=returnTag;
		return blockingUsers.add(user);
	}
	public boolean unregisterBlocking(Sim_entity user){
		return blockingUsers.remove(user);
	}


	Set<Sim_entity> blockingUsers=new LinkedHashSet<Sim_entity>();
	
	
	public Datum(String name, double size, double delta){
		this.id= totalId++;
		this.name=name;
		this.size=size;
		this.delta=delta;
	}
	
	public Datum(String name, Datum data){
		this(name, data.size,data.delta,data.records);
	}
	public Datum(Datum data){
		this(data.getName(), data);
	}
	public Datum(String name,double size, int parts){
		this(name, size, (double)size/parts);
	}
	public Datum(String name, double size, double delta, double records){
		this(name, size, delta);
		this.records=records;
	}
	public double percent(){
		return (double)currentSize/size;
	}
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Datum other = (Datum) obj;
		if (id != other.id)
			return false;
		return true;
	}
	
	public Datum addDatum(Datum data, double delta){
		double r=records+ data.records;
		double sz=size+ data.size;
		
		double rtu= (size* ratio + data.size* data.ratio)/sz;
		Datum result=new Datum(getName(), sz, delta, records);
		result.ratio=rtu;
		return result ;
	}
	
	@Override
	public String toString() {
		return 		this.getClass().getName()+" :"+name+"\t"+ id+
		" size: "+size+ " percent: "+ percent()+ " delta:"+delta+ " deltaSize:"+deltaList.size();
	}

	public Datum getDatum(double sz, double d){
		Datum result=new Datum("datum_"+getName(), sz, d, sz/size*records);
		return result;
	}
	
	
	
	public Datum recieve(Sim_entity entity){
		Sim_event ev=new Sim_event();
		entity.sim_get_next(predicate(returnTag), ev);
		return (Datum)ev.get_data();
	}
	
	private Sim_predicate predicate(int i){
		return new Sim_type_p(i);
		
	}
	
}
