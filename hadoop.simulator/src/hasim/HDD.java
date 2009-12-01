package hasim;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.swing.DebugGraphics;
import javax.xml.crypto.Data;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.datagrid.File;
import hasim.json.JsonHardDisk;



public class HDD extends GridSim{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HDD.class);


	double utilization;

	//	List<Disk> disks=new ArrayList<Disk>();
	//CircularList<Disk> disks=new CircularList<Disk>();
	Disk disk; //TODO one disk implementation need to change it to lis of disks
	

	//id, progress
	//Vector< Filelet> files=new Vector< Filelet>();
	CircularList<Datum> files=new CircularList<Datum>();

	public HDD(String name, JsonHardDisk j) throws Exception{
		super(name);
		this.disk=new Disk(j.getSeekTime(), j.getRead(),
				j.getWrite(), j.getCapacity());
	}
	public HDD(String name,Disk ... dsks) throws Exception {
		super(name);
		disk=dsks[0];
	}

	public int submit( double delta,double delay, Datum fileObject){
		//logger.debug("delta:"+ delta+ " for :"+ fileObject);
		assert(fileObject.tag== HTAG.read_tmp_delta || fileObject.tag==HTAG.write_tmp_delta);

		send(get_id(), delay, HTAG.hdd_add.id(),new DatumLocal(fileObject,delta));
		return files.size();
	}
	
	
	@Override
	public void body() {

		//int counter=0;
		
		while (Sim_system.running()) {
			
			//if( (counter++)> 30)break;
			
			Sim_event ev=new Sim_event();
			sim_get_next(ev);
			
			int tag= ev.get_tag();
			if( tag == GridSimTags.END_OF_SIMULATION){
				break;
			}
			

//			if(tag >= HTAG.HDBASE){
//				logger.debug("recieve event "+ HTAG.get(tag)+" at time "+GridSim.clock() );
//			}

			if( tag == HTAG.hdd_add.id()){
				DatumLocal data=(DatumLocal)ev.get_data();
				
				if(data.delta != 0)	data.datum.deltaList.addLast(data.delta);
				
				if(data.datum.deltaList.size()==0)continue;

				if( ! files.contains(data.datum) ){
					files.add(data.datum);
					//no jobs before
					if(files.size()==1)
						send(get_id(), 0.0, HTAG.hdd_check.id());
				}
				continue;
			}
			
			if( tag == HTAG.hdd_check.id()){
				if(files.size()==0)continue;
				Datum file=files.next();
				assert (file.deltaList.size()>0);

				double delta=file.deltaList.removeFirst();
				double speed = tag==HTAG.read_tmp_delta.id()? disk.read:disk.write;

				
				double t=delta/speed;
				//logger.info("time needed to process "+ t);
				sim_process(t);
				
				//added by suhel for blocking users
				Set<Sim_entity> blockingUsers=file.getBlockingUsers();
				
				for (Iterator<Sim_entity> iter = blockingUsers.iterator(); iter
						.hasNext();) {
					Sim_entity user = iter.next();
					send(user.get_id(), 0.0, file.returnTag,file);
					
				}
				
				if(file.deltaList.size()==0)files.remove(file);
				if(files.size()==0)continue;
				
				
				send(get_id(), 0.0, HTAG.hdd_check.id());
				
				//logger.debug("proccess "+ file + " at time :"+ GridSim.clock());
				continue;
			}

		}

	}

	@Override
	public String toString() {
		return "HDD:"+disk.toString();
	}
	

}


class Disk{
	final double seek;
	final double read;
	final double write;
	
	final double capacity;
	


	public Disk(double seek, double read, double write, double capacity) {
		this.seek = seek;
		this.read = read;
		this.write = write;
		this.capacity = capacity;
	}
	
	
	
	public double getTime(Datum file){
		double speed= file.tag == HTAG.read_tmp_delta ? read : write;
		//file.partsSent++;
		return file.tmpDelta/speed;
	}
	
	@Override
	public String toString() {
		StringBuffer sb=new StringBuffer(" seek:"+ seek+ 
				" read:"+ read +" write:"+ write+ " capacity:"+ capacity);
		return super.toString();
	}
	
}

class DatumLocal{
	public DatumLocal(Datum datum, double delta) {
		this.datum = datum;
		this.delta = delta;
	}
	
	
	public final Datum datum;
	public final double delta;
	public HTAG tag;
	
	@Override
	public String toString() {
		return datum.toString()+"\tdelta:"+delta;
	}
}


