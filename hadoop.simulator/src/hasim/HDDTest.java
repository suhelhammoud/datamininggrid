package hasim;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_event;

import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import gridsim.GridSim;

public class HDDTest extends GridSim {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HDDTest.class);

	List<Datum> files=new ArrayList<Datum>();
	HDD hdd;
	
	public HDDTest(String name) throws Exception {
		super(name);
		init();
	}
	
	private void init(){
		Datum f1= new Datum("f1",1000,20);
		Datum f2= new Datum("f2",500,5);
		Datum f3= new Datum("f3",500, 2);
		Datum f4= new Datum("f4",200,5);
		
		f1.tag=HTAG.read_tmp_delta;
		f2.tag=HTAG.write_tmp_delta;
		f3.tag=HTAG.read_tmp_delta;
		f4.tag=HTAG.read_tmp_delta;
		
//		f1.registerBlocking(this, f1.getId());
//		f2.registerBlocking(this, f2.getId());
//		f3.registerBlocking(this, f3.getId());
//		f4.registerBlocking(this, f4.getId());
		
		f1.registerBlocking(this, 4);
		f2.registerBlocking(this, 4);
		f3.registerBlocking(this, 4);
		f4.registerBlocking(this, 4);

		files.add(f1);
		files.add(f2);
		files.add(f3);
		files.add(f4);
		
		logger.info(f1);
		logger.info(f2);
		logger.info(f3);
		logger.info(f4);
	}

	public void fileComplete(Datum file) {
		logger.info("fileComplete "+ file + " at time "+ GridSim.clock());
		
	}

	public void fileProgress(Datum file) {
		logger.info("fileProgress "+ file + " at time "+ GridSim.clock());
		
	}
//	public void filetSubmit(Datum file, HDD hdd) {
//		logger.info("Going to submit file "+ file +" at time "+ GridSim.clock());
//		hdd.submint(file);
//		
//	}
	
	public void body() {
		logger.info("size="+ files.size());
		for (Datum file : files) {
			double delay=0;
			for (int i = 0; i < file.size/file.delta; i++) {
				file.deltaList.add(file.delta);
				//logger.debug("delta:"+ file.delta+" for :"+ file);
				//hdd.submit(file.delta, delay, file);
			}

			hdd.submit(0, delay, file);

		}
		
		for (Datum file : files) {
			for (int i = 0; i < file.size/file.delta; i++) {
				
//				Datum.recieve(this, HTAG.read_tmp_delta_return);
//				Sim_event ev=new Sim_event();
//				sim_get_next(HTAG.read_tmp_delta_return.predicate(), ev);
//				
				//Datum fileReturn=Datum.recieve(this, HTAG.cpu_tic);
				logger.info(">>>>>>>>>>>>>>>>>>> "+ file.recieve(this) +" at time :"+ GridSim.clock());
			}
		}
		
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
			
			Disk disk=new Disk(0, 10, 10, 50000);
			HDD hdd=new HDD("hdd_1	", disk);
			
			HDDTest user=new HDDTest("user_1");
			user.hdd=hdd;

						// Fourth step: Starts the simulation
			GridSim.startGridSimulation();

			logger.info("Finish Simulation");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Unwanted errors happen");
		}
	}

}
