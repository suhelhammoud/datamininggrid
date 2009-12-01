package hasim;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

import gridsim.GridSim;
import gridsim.GridSimTags;
import hasim.gui.HMonitor;
import hasim.json.JsonCpu;

public class CPU extends GridSim{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(CPU.class);
	public static double cpu_interval=1;


	double utilization;
	final int cores;
	

	final double speed;//MIPS

	HMonitor monitor;

	
	public void showMonitor(){
		monitor.setVisible(true);
	}

	//id, progress
	CircularList< Datum> jobs=new CircularList< Datum>();
	//Set<Datum> runningJobs=new HashSet<Datum>();

	public CPU(String name,JsonCpu jsnCpu) throws Exception{
		super(name);
		this.cores=jsnCpu.getCores();
		this.speed=jsnCpu.getSpeed();
		this.monitor=new HMonitor(name);
	}
	public CPU(String name,int numOfCores, double speed) throws Exception {
		super(name);
		this.cores=numOfCores;
		this.speed= speed;
		this.monitor=new HMonitor(name);

	}

	
	
	public int submint(double delta, double delay, Datum job){
		//logger.debug("delta:"+ delta+ " for :"+ fileObject);
		assert(job.tag== HTAG.cpu );

		send(get_id(), delay, HTAG.joblet_submit_local.id(),
				new DatumLocal(job,delta));
		return jobs.size();
	}
	


	@Override
	public void body() {

		int coutner=0;

		HashSet<Datum> runningJoblets=new HashSet<Datum>(cores);
		
		while (Sim_system.running()) {
			if((coutner++)>2000)break;

			monitor.log("cores", Sim_system.clock(), ""+runningJoblets.size(), false);

			Sim_event ev=new Sim_event();
			sim_get_next(ev);

			int tag= ev.get_tag();



			if( tag == GridSimTags.END_OF_SIMULATION){
				break;
			}
		

			{

				logger.debug("event "+ HTAG.get(tag)+ 
						" with data: "+ ev.get_data()+" at time "+GridSim.clock() );
			}

			if ( tag == HTAG.joblet_submit_local.id()){
				DatumLocal data=(DatumLocal)ev.get_data();

				if(data.delta != 0)
					data.datum.deltaList.addLast(data.delta);

				if(data.datum.deltaList.size()==0)
					continue;

				//job already running before
				if( jobs.contains(data.datum))
					continue;

				{
					jobs.add(data.datum);
					//all cores occupied nothing to do

					if(runningJoblets.size()==cores)
						continue;


					//at least one available core ready to take the delta job
					assert(runningJoblets.size() < cores);
					runningJoblets.add(data.datum);

					double time=data.datum.deltaList.getFirst()/speed;

					send(get_id(), time, HTAG.cpu_check.id(), data.datum);

				}
				continue;


			}
			logger.debug("cores:"+ runningJoblets.size() + " jobs:"+ jobs.size());
			

			if(tag == HTAG.cpu_check.id()){
				Datum job=(Datum)ev.get_data();

				//done the job return tags
				job.tmpDelta=job.deltaList.removeFirst();

				for (Sim_entity user : job.getBlockingUsers())
					send(user.get_id(), 0.0, job.returnTag,job);				


				if(job.deltaList.size()==0){
					//Job done, remove it from jobs and from cores	
					jobs.remove(job);
					runningJoblets.remove(job);

					//check if other jobs need the available core
					if( jobs.size() > runningJoblets.size()){
						Datum nextJob=jobs.next();
						while( runningJoblets.contains(nextJob))
							nextJob =jobs.next();

						runningJoblets.add(nextJob);
						double time= nextJob.deltaList.getFirst()/speed;
						send(get_id(), time, HTAG.cpu_check.id(), nextJob);
						continue;
					}
					continue;

				}else{
					// job is not done

					if( jobs.size() <= runningJoblets.size()){
						double time= job.deltaList.getFirst()/speed;
						send(get_id(), time, HTAG.cpu_check.id(), job);
						continue;
					}


					//if( jobs.size() > runningJoblets.size()){
					//switch jobs in the core
					assert(job != null);
					runningJoblets.remove(job);

					Datum nextJob=jobs.next();
					while( runningJoblets.contains(nextJob) )
						nextJob=jobs.next();

					runningJoblets.add(nextJob);
					double time= nextJob.deltaList.getFirst()/speed;
					send(get_id(), time, HTAG.cpu_check.id(), nextJob);
					continue;

				}


			}

		}


	}
	
	@Override
	public String toString() {
		return "CPU: speed:"+speed+", cores:"+cores;
	}
}



