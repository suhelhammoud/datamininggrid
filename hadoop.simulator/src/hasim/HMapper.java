package hasim;



import java.util.LinkedList;
import java.util.Random;

import dfs.HSplit;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;
import gridsim.GridSim;
import gridsim.GridSimTags;
import hasim.gui.HMonitor;
import hasim.gui.SimoTree;
import hasim.gui.SimoTree.TreeIndex;
import hasim.json.JsonSplit;


import org.apache.log4j.Logger;



public class HMapper extends GridSim{
	/**
	 * Logger for this class
	 */

	private static final Logger logger = Logger.getLogger(HMapper.class);
	

	private final JobInfo job;
	static SimoTree simoTree;
	
	public JobInfo getJob() {
		return job;
	}

	public int treeIndex;
	public int treeIndexList;


	public boolean done=false;
	
	private HMonitor monitor;
	private HCounter counters=new HCounter();
	
	enum TypeTag{isMap, isReducer, isCombiner};
	
	
	
	public enum Phase {	MAP, SHUFFLE, REDUCE	}
	public enum State{fetch, buffer, buffer_sort, buffer_combine,
		buffer_spill , sort, combine, spill};


		LinkedList<Datum> spills=new LinkedList<Datum>();

		int minSpills;

		State state=State.fetch;

		// Counters used by Task subclasses
		
		HCombiner combiner;
		HReducer reducer;


		public HTaskTracker tracker;
		HBuffer memBuffer;
		private HSplit split;
		private JsonSplit jsnSplit;

		//Netlet netlet;


		public JsonSplit getJsnSplit() {
			return jsnSplit;
		}



		public void setJsnSplit(JsonSplit jsnSplit) {
			this.jsnSplit = jsnSplit;
		}



		HJobConf conf;
		long taskID;


		public HMapper(String name, JobInfo job)throws Exception{
			super(name);
			this.job=job;
			
			this.monitor=new HMonitor(name);
		}
//		public HMapper(String name,HTaskTracker machine, HSplit split) throws Exception {
//			super(name);
//			this.machine=machine;
//			this.split=split;
//
//		}



		public void startMapper(HTaskTracker tracker){
			this.tracker=tracker;
			tracker.decMappers();
			send(get_id(), 0.0, HTAG.START.id());
			
			job.mappersWaiting.remove(this);
			job.mappersRunning.add(this);
			
			counters.put(CounterTag.START_TIME, Sim_system.clock());
			
			if(simoTree ==null)return;
			simoTree.moveNode(this, TreeIndex.mRunning);


		}
		
		public void stopMapper(){
			job.mappersRunning.remove(this);
			job.mappersFinished.add(this);
			tracker.incMappers();
			
			counters.put(CounterTag.STOP_TIME, Sim_system.clock());
			
			if(simoTree ==null)return;
			simoTree.moveNode(this, TreeIndex.mFinished);
			
		}

		public void showMonitor(){
			monitor.setVisible(true);
		}
		@Override
		public void body() {
			
			//wait start signal
			//monitor.setVisible(true);
			monitor.txt(Sim_system.clock(), "wait for start tag");

			Sim_event evStart=new Sim_event();
			sim_get_next(HTAG.START.predicate(), evStart);

			logger.debug("receive event "+ HTAG.toString(evStart.get_tag()));
			
			logger.debug("tracker:"+tracker.toString());
			
			
//			tracker.lock.lock();
//			tracker.decMappers();
//			tracker.lock.unlock();
//			
			
			
			///do the job
			Random rnd=new Random();
			double t=5+rnd.nextInt(10);
			//monitor.step(Sim_system.clock(),"waiting untill ");
			//sim_process(t);
			Datum dtm=new Datum("datum_"+job.getId(), 100, 100);
			dtm.tag=HTAG.cpu;
			dtm.registerBlocking(this, HTAG.mapper_cpu.id());

			for (int i = 0; i < 10; i++) {
				tracker.cpu.submint(1000.0+ rnd.nextDouble()*200, 0.0, dtm);
			}
			

			for (int i = 0; i < 10; i++) {
				dtm.recieve(this);
			}
			

			//monitor.step(Sim_system.clock(),"finish mapper:"+get_name());

			monitor.txt(Sim_system.clock(), job.mappersRunning.toString());
			
//			//finalize
//			job.lock.lock();
//			try {
//				job.mappersRunning.remove(this);
//				job.mappersFinished.add(this);
//			} catch (Exception e) {
//			}finally{ job.lock.unlock();};
//			
			
			done=true;
			
			logger.debug("Finish Mapper "+ this.get_name());
			if(true)return;
			
			Sim_event ev=new Sim_event();

			while(Sim_system.running()){

				//fetch the split over the network
				split.registerBlocking(this, split.id());
				int parts= (int)(split.size/split.delta);

				for (int i = 0; i < parts; i++) {
					split.deltaList.addLast(split.delta);
				}
				if( split.getLocation().equals(tracker.jsn.getHostName())){
					tracker.hdd.submit(0.0, 0.0, split);
				}else{
					for (double d : split.deltaList) 
						NetEnd.sendData(split.getLocation(), tracker.jsn.getHostName(),
								split.delta, split, 0.0);
				}


				Datum job=new Datum("job_"+get_name(), split.size* Algorithm.mapCost, 
						split.delta* Algorithm.mapCost,split.records* Algorithm.mapRecords);

				Datum mapOutput=new Datum("mapOutput_"+get_name(), split.size* Algorithm.mapSize,
						split.delta* Algorithm.mapSize, split.records*Algorithm.mapRecords);

				//			for (int i = 0; i < parts; i++) {
				//				job.deltaList.addLast(job.delta);
				//			}
				job.registerBlocking(this, job.id());

				for (int i = 0; i < parts; i++) {
					Datum splitRDatum=split.recieve(this);

					tracker.cpu.submint(split.delta*Algorithm.mapCost, 0.0, job);

					Datum jobR= job.recieve(this);
					send(memBuffer.get_id(), 0.0, HTAG.mem_add.id(),
							new Double(mapOutput.delta));

					memBuffer.receiveAddReturn(this);

				}
				//finish the map job
				send(memBuffer.get_id(),0.0, HTAG.mem_flush.id());

				//wait untill the spiller_end received
				Sim_event eSpillerEnd=new Sim_event();

				sim_get_next(HTAG.spiller_end.predicate(), eSpillerEnd);


				// merg spills until less that minSpills
				//start merger Entity
				LinkedList<Datum> spillsMerg=new LinkedList<Datum>();
				LinkedList<Datum> spillsOut=new LinkedList<Datum>();

				int mrgParts=10;

				for (int i = 0; i < spills.size(); i++) {

					Datum spill=spills.get(i);
					Datum mrgSpill=new Datum("mrg_"+i, spill.size, spill.size/mrgParts);

					for (int j = 0; j < mrgParts; j++) {
						mrgSpill.deltaList.add(mrgSpill.delta);
					}
					mrgSpill.registerBlocking(this, split.id());

					spillsMerg.addLast(mrgSpill);
				}

				while(spillsMerg.size()> minSpills){
					logger.info("Merg "+  minSpills+" spills from spillsMerg of size:"+ spillsMerg.size());
					double sum=0;

					for (int i = 0; i < minSpills; i++){
						double sz = spillsMerg.get(i).size;
						sum +=sz;
						tracker.hdd.submit(sz, 0.0, spillsMerg.get(i));
					}

					Datum out = new Datum("mrg_"+spillsOut.size(), sum, sum);
					out.registerBlocking(this, out.id());

					tracker.hdd.submit(sum, 0.0, out);

					//wait untill read minSpills and merg to out
					for (int i = 0; i < minSpills; i++) 
						spillsMerg.get(i).recieve(this);
					out.recieve(this);

					for (int i = 0; i < minSpills; i++){
						spillsMerg.removeFirst();
					}
					spillsMerg.addLast(out);

				}

				done=true;
				//end the map task call the tasktracker to start the reducer


			}

		}
		
		@Override
		public String toString() {
			
			return get_name();
		}
		
		public String info1() {
			StringBuffer sb=new StringBuffer("Mapper:"+ get_name());
			sb.append(",\tTracker:"+tracker.get_name());
			
			return sb.toString();
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
}



