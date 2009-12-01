package hasim;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import gridsim.GridSim;
import gridsim.GridSimTags;
import hasim.gui.HMonitor;
import hasim.json.JsonMachine;

public class HTaskTracker extends GridSim{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HTaskTracker.class);
	
	//public static Map<String, HTaskTracker> taskTrakers;

	//public HResourceStatus resStatus=new HResourceStatus();

	HJobTracker jobTracker;
	public HMonitor monitor;
	
	public Lock lock=new ReentrantLock(true);
	
	public final CPU cpu;
	public final NetEnd netend;
	public final HDD hdd;
	public final JsonMachine jsn;

	
	private int aMappers,aReducers;
	
	public int incMappers(){
		lock.lock();
		aMappers++;
		int r=aMappers;
		lock.unlock();
		return r;
	}
	
	public int decMappers(){
		assert aMappers > 0: aMappers;

		lock.lock();
		aMappers--;
		int r=aMappers;
		lock.unlock();
		return r;
	}
	
	public int incReducers(){
		lock.lock();
		aReducers++;
		int r=aReducers;
		lock.unlock();
		return r;
	}
	
	public int decReducers(){
		assert aReducers > 0: aReducers;

		lock.lock();
		aReducers--;
		int r=aReducers;
		lock.unlock();
		return r;
	}

	List<HMapper> mappers=new ArrayList<HMapper>();
	List<HShuffle> shufflers=new ArrayList<HShuffle>();
	List<HReducer> reducers=new ArrayList<HReducer>();
	
	

	

	
	public HTaskTracker( JsonMachine j) throws Exception{
		super(j.getHostName());
		this.cpu=new CPU("cpu_"+j.getHostName() , j.getCpu());
		this.hdd=new HDD("hdd_"+j.getHostName(), j.getHardDisk());
		this.netend=NetEnd.netends.get(j.getHostName());
		this.jsn=j;
		
		
		this.aMappers=jsn.getMaxMapper();
		this.aReducers=jsn.getMaxReducer();
		
		logger.debug("aMappers:"+ aMappers);
		logger.debug("aReducers:"+ aReducers);
		
		monitor=new HMonitor(get_name());
		monitor.step("init monitor ");
		
	}
	
	@Override
	public void body() {

		while(Sim_system.running()){
			
			Sim_event ev=new Sim_event();
			sim_get_next(ev);
			int tag=ev.get_tag();
			
			if( tag== GridSimTags.END_OF_SIMULATION){
				break;
			}
			
			if( tag== HTAG.heartbeat.id()){
				
			}
		}
	}
	
	public void finishJob(Object ojob){
		if( ojob instanceof HMapper){
			mappers.add((HMapper) ojob);
		}else if( ojob instanceof HReducer){
			reducers.add((HReducer) ojob);
		}else if( ojob instanceof HShuffle){
			shufflers.add((HShuffle) ojob);
		}else{
			logger.error(" UFO object :) ");
		}
	}
	public int getaMappers() {
		return aMappers;
	}

	public void setaMappers(int aMappers) {
		this.aMappers = aMappers;
	}

	public int getaReducers() {
		return aReducers;
	}

	public void setaReducers(int aReducers) {
		this.aReducers = aReducers;
	}
	
	public String toString() {
		return get_name();
	}
	
	public String toStringInfo() {
		StringBuffer sb=new StringBuffer("tracker:"+get_name());
		sb.append("\taMappers:"+ aMappers+ "aReducers:"+ aReducers);
		return sb.toString();
	}
	
}
