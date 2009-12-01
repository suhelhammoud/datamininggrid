package hasim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreePath;

import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonIgnore;

import com.lowagie.text.pdf.codec.JBIG2Image;

import dfs.HSplit;

import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

import gridsim.GridSim;
import gridsim.GridSimTags;
import hasim.gui.HMonitor;
import hasim.gui.MObject;
import hasim.gui.SimoTree;
import hasim.gui.SimoTree.TreeIndex;
import hasim.json.JsonConfig;
import hasim.json.JsonJob;
import hasim.json.JsonMachine;
import hasim.json.JsonRealRack;
import hasim.json.JsonSplit;

public class HJobTracker extends GridSim {
	private static final Logger logger = Logger.getLogger(HJobTracker.class);
	public static JsonConfig config;

	public List<JobInfo> jobsFinished = new ArrayList<JobInfo>();

	public List<JobInfo> jobsRunning = new ArrayList<JobInfo>();

	public List<JobInfo> jobsWaiting = new ArrayList<JobInfo>();

	public Lock lock = new ReentrantLock(true);

	public HMonitor monitor, m=new HMonitor("all");
	
	public static SimoTree simoTree;
	
	private Map<String, HTaskTracker> trackers = new LinkedHashMap<String, HTaskTracker>();

	
	public HJobTracker(String name) throws Exception {
		super(name);
		this.monitor = new HMonitor(name);
		
	}

	
	public JobInfo addJob(JsonJob j) {
		monitor.txt(Sim_system.clock(), "add job" + j);
		if (j.getData() != null) {
			// split it to the number of mappers
			JsonSplit data = j.getData();
			if (!data.getReplica().contains(data.getLocation())) {
				data.getReplica().add(data.getLocation());
			}

			for (int max = j.getNumberOfMappers(), i = 0; i < max; i++) {
				JsonSplit jSplit = new JsonSplit();
				jSplit.setId("split_" + j.getJobName());
				jSplit.setLocation(data.getLocation());
				jSplit.setSize(data.getSize() / max);
				jSplit.setRecords(data.getRecords() / max);

				j.getInputSplits().add(jSplit);
			}
		}

		JobInfo job = new JobInfo(j);
		
		job.simoTree=simoTree;
		
		job.startJob(Sim_system.clock());
		
		send(get_id(), 0.0, HTAG.job_tracker_add_job.id(), job);
		

		return job;
	}

	private void addJobLocal(JobInfo job) throws Exception {

		int rSz = job.getJson().getNumberOfReducers();

		List<JsonSplit> jsnSplits = job.json.getInputSplits();

		for (int i = 0; i < jsnSplits.size(); i++) {
			HMapper mapper = new HMapper("mapper_" + i + "_"
					+ job.getJson().getJobName(), job);
			mapper.simoTree=simoTree;
			
			mapper.setJsnSplit(jsnSplits.get(i));
			job.mappersWaiting.add(mapper);

		}

		for (int i = 0; i < rSz; i++) {
			HReducer reducer = new HReducer("reducer_" + i + "_"
					+ job.getJson().getJobName(), job);
			
			reducer.simoTree=simoTree;
			job.reducersWaiting.add(reducer);
		}

		jobsWaiting.add(job);
		monitor.txt(Sim_system.clock(), "addLocal Job:" + job);
		
		if(simoTree != null)
			simoTree.addJob(job);
	
	}

	@Override
	public void body() {
		simoTree.addRack("rack 01", trackers);

		send(get_id(), config.getHeartbeat(), HTAG.heartbeat.id());

		//	
		// Sim_event e2 = new Sim_event();
		// sim_get_next(HTAG.START.predicate(),e2);
		//		
		while (Sim_system.running()) {
			Sim_event ev = new Sim_event();
			sim_get_next(ev);
			int tag = ev.get_tag();

			double time = Sim_system.clock();

			monitor.step(Sim_system.clock(), "event:" + HTAG.toString(tag));

			if (tag == GridSimTags.END_OF_SIMULATION) {
				logger.info("receive end of simulation");
				break;
			}

			if (tag == HTAG.job_tracker_add_job.id()) {
				try {
					addJobLocal((JobInfo) ev.get_data());
					monitor.txt(Sim_system.clock(), "add job " + ev.get_data());

				} catch (Exception e) {
					e.printStackTrace();
				}
				continue;
			}

			if (tag == HTAG.heartbeat.id()) {
				send(get_id(), config.getHeartbeat(), HTAG.heartbeat.id());

				monitor.log(MObject.jobsWaiting.name(), Sim_system.clock(), ""
						+ jobsWaiting.size(), false);
				monitor.log(MObject.jobsRunning.name(), Sim_system.clock(), ""
						+ jobsRunning.size(), false);
				monitor.log("jobsFinished", Sim_system.clock(), ""
						+ jobsFinished.size(), false);

				checkMappersRunning();

				checkReducersRunning();

				startNewMappers(jobsRunning, getAMappers());

				startNewMappers(jobsWaiting, getAMappers());

				startNewReducers();

				updateJobs();

				// assign new tasks if needed

				// send(get_id(), config.getHeartbeat(), HTAG.heartbeat.id());
				continue;
			}
		}
	}

	private void checkMappersRunning() {
		logger.debug("time:" + Sim_system.clock());
		for (JobInfo job : new ArrayList<JobInfo>(jobsRunning)) {
			for (HMapper mapper : new ArrayList<HMapper>(job.mappersRunning)) {
				if (mapper.done) {
					mapper.stopMapper();
				}
			}
		}
	}

	private void checkReducersRunning() {
		logger.debug("time:" + Sim_system.clock());
		for (JobInfo job : new ArrayList<JobInfo>(jobsRunning)) {
			for (HReducer reducer : new ArrayList<HReducer>(job.reducersRunning)) {
				if (reducer.done) {
					reducer.stopReducer();
				}
			}
		}

	}

	public List<String> getAMappers() {
		Map<String, Integer> map = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, HTaskTracker> e : trackers.entrySet()) {
			map.put(e.getKey(), e.getValue().getaMappers());
		}

		return Tools.scater(map);
	}

	public List<String> getAReducers() {
		Map<String, Integer> map = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, HTaskTracker> e : trackers.entrySet()) {
			map.put(e.getKey(), e.getValue().getaReducers());
		}
		return Tools.scater(map);
	}

	public void initSimulation(String configFile, String rackFile)
			throws Exception {

		if (configFile == null)
			configFile = "data/json/config.json";
		if (rackFile == null)
			rackFile = "data/json/rack_working.json";

		config = JsonConfig.readFrom(configFile);
		JsonRealRack rack = JsonRealRack.readFile(rackFile);

		logger.info("config:" + configFile + ",\tread rackFile:" + rackFile);

		// init the network topology
		logger.info("init NetEnd from rack");
		NetEnd.init(rack);

		for (JsonMachine m : rack.getMachines()) {
			
			HTaskTracker machine = new HTaskTracker(m);
			trackers.put(m.getHostName(), machine);
			logger.info("add machine "+ m.getHostName());
		}
		logger.info("add trackers:" + trackers.size());
		
	}

	
	private void startNewMappers(List<JobInfo> aJobs, List<String> aMappers) {

		logger.debug("available mappers " + aMappers);

		out: for (JobInfo job : new ArrayList<JobInfo>(aJobs)) {
			for (HMapper mapper : new ArrayList<HMapper>(job.mappersWaiting)) {
				if (aMappers.size() == 0)
					break out;

				JsonSplit split = mapper.getJsnSplit();
				assert split != null;
				if (aMappers.contains(split.getLocation())) {

					mapper.startMapper(trackers.get(split.getLocation()));

					aMappers.remove(split.getLocation());
				} else {
					mapper.startMapper(trackers.get(aMappers.remove(0)));

				}

			}
		}
	}

	private void startNewReducers() {

		List<String> aReducers = getAReducers();
		logger.debug("available reducers " + aReducers);


		out: for (JobInfo job : jobsRunning) {
			// do not start reduce if there is mappers waiting or running
			if (job.mappersRunning.size() != 0
					|| job.mappersWaiting.size() != 0) {
				continue;
			}

			// check only jobs running
			
			for (HReducer reducer : new ArrayList<HReducer>(job.reducersWaiting)) {
				
			
				if (aReducers.size() == 0)
					break out;

				reducer.startReducer(trackers.get(aReducers.remove(0)));

			}
		}

	}

	public void startSimulation() {
		send(get_id(), 0.0, HTAG.heartbeat.id());
		send(get_id(), 0.0, HTAG.START.id());

	}

	private void updateJobs() {
		// move from waiting to running
		for (Iterator<JobInfo> iter = jobsWaiting.iterator(); iter.hasNext();) {
			JobInfo job = iter.next();
			if (job.mappersRunning.size() > 0) {
				iter.remove();
				jobsRunning.add(job);
				
				if(simoTree !=null){
					simoTree.moveNode(job, TreeIndex.mRunning);
				}

			}
		}

		// move from running to finished
		for (Iterator<JobInfo> iter = jobsRunning.iterator(); iter.hasNext();) {
			JobInfo job = iter.next();
			if (job.reducersFinished.size() == job.json.getNumberOfReducers()) {
				iter.remove();
				jobsFinished.add(job);
				job.stoptJob(Sim_system.clock());
				
				if(simoTree !=null){
					simoTree.moveNode(job, TreeIndex.mFinished);
				}
			}
		}
		
		
		
	}
	
	private void updateJobInfos(){
		for (JobInfo job:jobsWaiting) {
			
		}
	}

}
