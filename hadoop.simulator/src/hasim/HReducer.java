package hasim;

import org.apache.log4j.Logger;

import java.util.Random;

import javax.swing.tree.DefaultTreeModel;

import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;
import gridsim.GridSim;
import gridsim.GridSimTags;
import hasim.gui.HMonitor;
import hasim.gui.SimoTree;
import hasim.gui.SimoTree.TreeIndex;

public class HReducer extends GridSim{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HReducer.class);
	
	public DefaultTreeModel treeModel;
	public int treeIndex;
	public int treeIndexList;
	static SimoTree simoTree;

	
	private final  JobInfo job;
	public HTaskTracker tracker;
	HMonitor monitor;
	private HCounter counters=new HCounter();

	
	public boolean done;
	public HReducer(String name, JobInfo job) throws Exception {
		super(name);
		this.job=job;
		this.monitor=new HMonitor(name);
	}
	public JobInfo getJob() {
		return job;
	}
	
	
	public void startReducer(HTaskTracker tracker){
		this.tracker=tracker;
		tracker.decReducers();
		send(get_id(), 0.0, HTAG.START.id());
		
		job.reducersWaiting.remove(this);
		job.reducersRunning.add(this);
		counters.put(CounterTag.START_TIME, Sim_system.clock());

		if(simoTree ==null)return;
		simoTree.moveNode(this, TreeIndex.rRunning);


	}
	
	public void stopReducer(){
		job.reducersRunning.remove(this);
		job.reducersFinished.add(this);
		tracker.incReducers();
		counters.put(CounterTag.STOP_TIME, Sim_system.clock());
		
		if(simoTree ==null)return;
		simoTree.moveNode(this, TreeIndex.rFinished);


	}
	
	public void showMonitor(){
		monitor.setVisible(true);
	}
	@Override
	public void body() {
		//wait start signal

		sim_get_next(HTAG.START.predicate(), new Sim_event());
		//monitor.step("received start");
		
		
		
		///do the job
		Random rnd=new Random();
		double t=6;//70+rnd.nextInt(10);
		//monitor.step(Sim_system.clock(), "waiting untill "+t);
		Datum dtm=new Datum("datum_r"+job.getId(), 100, 100);
		dtm.tag=HTAG.cpu;
		dtm.registerBlocking(this, HTAG.mapper_cpu.id());

		monitor.txt(Sim_system.clock(), "start reducer");
		for (int i = 0; i < 10; i++) {
			tracker.cpu.submint(500.0+ rnd.nextDouble()*200, 0.0, dtm);
		}
		

		for (int i = 0; i < 10; i++) {
			
			dtm.recieve(this);
			monitor.log("test", Sim_system.clock(), "1", false);
		}
//		sim_process(t);

		monitor.txt(Sim_system.clock(), "finish reducer ");

		done=true;
		if(true)return;

		//finalize
	
		
		while (Sim_system.running()) {
			Sim_event ev = new Sim_event();
			sim_get_next(ev);
			int tag = ev.get_tag();

			if (tag == GridSimTags.END_OF_SIMULATION) {
				break;
			}
		}
		
		
		
	}

	public String info(){
		StringBuffer sb=new StringBuffer("["+get_name());
		sb.append(",\tid:"+get_id());
		sb.append(",\t jobId:"+job.getId());
		sb.append(",\t Tracker:"+tracker.get_name());
		sb.append(",\t Start:"+counters.get(CounterTag.START_TIME));
		sb.append(",\t Stop:"+counters.get(CounterTag.STOP_TIME));
		sb.append(",\t Tracker:"+tracker.get_name());
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		return get_name();
	}
}
