package hasim.json;

import org.apache.log4j.Logger;


import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_port;
import eduni.simjava.Sim_system;

import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.Gridlet;
import gridsim.Sim_from_port;

public class HPCombiner extends GridSim{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HPCombiner.class);

	Sim_port mem_port;
	Sim_port hdd_port;//TODO change it to link port (link with local file systm HDD
	
	private double delay=1;
	
	public enum state {START,END,READ,EXE,WRITE};
	
	public HPCombiner(String name) throws Exception {
		super(name);
		Sim_system.getEntityList();
		mem_port=new Sim_port("mem_port");
		hdd_port=new Sim_port("hdd_port");
		add_port(mem_port);
		add_port(hdd_port);
	}
	
	
	@Override
	public void body() {
		Sim_event ev = new Sim_event();
		while (Sim_system.running()) {
			super.sim_get_next(ev);

			// if the simulation finishes then exit the loop
			if (ev.get_tag() == GridSimTags.END_OF_SIMULATION) {
				logger.info("receive end of simulation event");
				break;
			}

			// process the received event
			logger.info("recive event "+ ev.get_data()+ " at time "+ GridSim.clock());
		}

		// remove I/O entities created during construction of this entity
		super.terminateIOEntities();
	}	
	
	public void body2() {
		Sim_event ev = new Sim_event();

		int counter=0;
		//while(Sim_system.running()){
		for (sim_get_next(new Sim_from_port(mem_port), ev); ev.get_tag() != GridSimTags.END_OF_SIMULATION;sim_get_next(ev)) {
			receiveEventObject(mem_port);
			logger.info("\n\nrecieve from  "+ ev.get_src() + " at time "+ GridSim.clock());
			logger.info("gridsim Tag=" + ev.get_tag());

			String evData = (String) ev.get_data();
			logger.info("ev data :"+evData);

			// get the sender ID, i.e Example3 class
			logger.info("going to pause:"+ GridSim.clock());
			sim_pause(delay);
			logger.info("after "+ GridSim.clock());
			//super.send(ev.get_src(), 1.0,   3, ""+GridSim.clock());

			sim_completed(ev);
			//if((counter++)> 10)break;
			
//			sim_get_next(new Sim_from_port(mem_port), ev);
//			logger.info("second event "+ ev.get_data());
			//break;
			// sends back the modified Gridlet to the sender
//			
		}
//		super.send(entityID, GridSimTags.SCHEDULE_NOW,
////				GridSimTags.GRIDLET_RETURN, evData);

		// when simulation ends, terminate the Input and Output entities
		logger.info("suhtdown and terminiate io");
	}
	

}
