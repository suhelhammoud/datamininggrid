package hasim;


import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

import gridsim.GridSim;
import gridsim.GridSimTags;
import hasim.json.JsonAlgorithm;

public class HSpiller extends GridSim{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HSpiller.class);

	AtomicBoolean isRunning;

	HBuffer buffer;
	HDD hdd;
	HCombiner combiner;
	JobInfo job;
	JsonAlgorithm alg;

	public void setHBufferHDDHCombiner(HBuffer buffer, HDD hdd, 
			HCombiner combiner){
		this.buffer=buffer;
		this.combiner=combiner;
		this.hdd=hdd;
	}

	public HSpiller(String name) throws Exception {
		super(name);
	}

	public boolean isRunning() {
		// TODO Auto-generated method stub
		return isRunning.get();
	}

	@Override
	public void body() {
		while(Sim_system.running()){
			Sim_event ev=new Sim_event();
			sim_get_next(ev);

			int tag = ev.get_tag();

			if( tag== GridSimTags.END_OF_SIMULATION){
				logger.debug("end of simulation");
				break;
			}

			if( tag== HTAG.spiller_start.id()){

				isRunning.set(true);

				Datum data=(Datum)ev.get_data();

				//start sort in memory
				double sortRecords=data.records;

				double sortCost=alg.getCombineCost()* data.records;

				Datum jobSort=new Datum("sort_"+getName(), sortCost, sortCost);
				jobSort.registerBlocking(this, jobSort.id());


				Datum jobSortReturn= jobSort.recieve(this);

				assert(jobSortReturn.getName().startsWith("sort_"));

				// start combine if necessary
				double outSize= data.size;
				double outRecords=data.records;
				double outCost;

				if(combiner != null){
					//start combiner
					//calc cominer cost
					outSize=alg.getCombineSize()* data.size;
					outRecords=alg.getCombineRecords()* data.records;
					outCost =alg.getCombineCost()* data.records;

					Datum jobCombine=new Datum("combine_"+getName(), outCost, outCost);
					jobCombine.registerBlocking(this, jobCombine.id());

					Datum jobCombineReturn= jobCombine.recieve(this);
					assert(jobCombineReturn.getName().startsWith("combine_"));

				}

				//TODO add compression job here is needed


				//write output to harddisk
				Datum outSpill=new Datum("spill", outSize, outSize);
				outSpill.registerBlocking(this, outSpill.id());
				Datum outSpillReturn=outSpill.recieve(this);

				assert(outSpillReturn.id() == outSpill.id());

				//send reset to the buffer
				send(buffer.get_id(), 0.0, HTAG.mem_reset.id());

				isRunning.set(false);
				continue;

			}

			//print other tag messages
			if(tag >= HTAG.HDBASE)
				logger.debug("event "+ HTAG.get(tag) + " at time :"+ GridSim.clock() );
			else
				logger.debug("event "+tag + " at time :"+ GridSim.clock() );



		}
	}

}
