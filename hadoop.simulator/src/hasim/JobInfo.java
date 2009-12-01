package hasim;

import hasim.gui.SimoTree;
import hasim.json.JsonJob;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeModel;

import dfs.HSplit;
import eduni.simjava.Sim_system;

public class JobInfo {

	public static enum State{START,MAP, MAP_D,SHUFFLE, SHUFFLE_D,
		REDUCE, REDUCE_D, FINISH, FINISH_D}

	JsonJob json;
	private static int TOTALID=0;
	public Lock lock=new ReentrantLock(true);
	
	static SimoTree simoTree;
	
	public State state=State.START;
	
	private int id;
	public static DefaultTreeModel treeModel;
	public int treeIndex;
	public int treeIndexList;
	
	public DefaultTreeModel getTreeModel() {
		return treeModel;
	}

	


	private HCounter couters=new HCounter();

	public JsonJob getJson() {
		return json;
	}

	public JobInfo() {
		this.id= (TOTALID++);
	}

	public int getId(){
		return this.id;
		
	}
	public JobInfo(JsonJob jsn) {
		this.json=jsn;
		this.id= (TOTALID++);

	}
	public void setJson(JsonJob json) {
		this.json = json;
	}


	HCounter counters=new HCounter();

	public List<HMapper> mappersWaiting=new ArrayList<HMapper>();
	public List<HMapper> mappersRunning=new ArrayList<HMapper>();
	public List<HMapper>  mappersFinished=new ArrayList<HMapper>();;
	
	public List<HReducer> reducersWaiting= new ArrayList<HReducer>();
	public List<HReducer> reducersRunning= new ArrayList<HReducer>();
	public List<HReducer> reducersFinished= new ArrayList<HReducer>();
	

	


	public List<Datum> spills=new ArrayList<Datum>();
	public List<Datum> merges=new ArrayList<Datum>();
	
	
	public void startJob(double time){
		counters.put(CounterTag.START_TIME, time);
	}
	public void stoptJob(double time){
		counters.put(CounterTag.STOP_TIME, time);
	}

	@Override
	public String toString() {
		return json.getJobName()+"-"+getId();
	}
	
	
}

