package dfs;

import java.util.Calendar;

import org.apache.log4j.Logger;


import dfs.Pair.Type;

import eduni.simjava.Sim_port;
import eduni.simjava.Sim_system;
import gridsim.GridSim;
import gridsim.GridSimTags;
import hasim.json.HPCombiner;
import hasim.json.TestCombiner;

public class HardDriveTest extends GridSim {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HardDriveTest.class);

	Sim_port out;

	double[] sizes={50,100,600,10,777};
	boolean[] read= { false,true, false, false, true};

	public HardDriveTest(String name) throws Exception {
		super(name);
	}

	public void attachHDD(HardDrive hd){
		out=new Sim_port("out");
		add_port(out);
		
		Sim_system.link_ports(this.getEntityName(),
				"out", hd.getEntityName(), "in");

	}

	public void init(){
		sizes=new double[]{40,200,300,10,888};
		read= new boolean[]{ true, false, false, true,false};
	}

	public double readFile(String fileName, double size){
		double delta=2;
		int numOfDeltas=(int)(size/delta);
		double rest=size-delta* numOfDeltas;
		
		return GridSim.clock();
	}
	@Override
	public void body() {
		for (int i = 0; i < sizes.length; i++) {
			Pair.Type t=read[i]?Type.READ:Type.WRTIE;

			Pair p=new Pair(t, sizes[i]);
			logger.info(getEntityName()+" sending "+ p+ " at time "+ GridSim.clock() );
			super.send(out, 0.0, 3, p);

			String ak=(String)super.receiveEventObject();
			logger.info(ak +" at time "+ GridSim.clock());
		}

		super.send(out, 0.33, GridSimTags.END_OF_SIMULATION);
		//
		//		shutdownGridStatisticsEntity();
		//		shutdownUserEntity();
	}

	public static void main(String[] args) {


		try {

			int num_user = 0; // number of users need to be created
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = true; // mean trace GridSim events

			String[] exclude_from_file = { "" };
			String[] exclude_from_processing = { "" };
			String report_name = null;

			// Initialize the GridSim package
			logger.info("Initializing GridSim package");
			GridSim.init(num_user, calendar, trace_flag, exclude_from_file,
					exclude_from_processing, report_name);

			HardDrive hdd=new HardDrive("hard", 200000);
			HardDriveTest test1=new HardDriveTest("test1");
			test1.attachHDD(hdd);
			HardDriveTest test2=new HardDriveTest("test2");
			test2.attachHDD(hdd);
			test2.init();


			// Fourth step: Starts the simulation
			GridSim.startGridSimulation();


			logger.info("Finish Simulation");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Unwanted errors happen");
		}
	}
}
