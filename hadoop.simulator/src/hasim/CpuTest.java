package hasim;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_event;
import eduni.simjava.Sim_type_p;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import gridsim.GridSim;

public class CpuTest extends GridSim  {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(CpuTest.class);

	public CpuTest(String name, CPU cpu) throws Exception {
		super(name);
		this.cpu = cpu;
	}

	public static void main(String[] args) {
		// test1();
		// if(true)return;
		//		
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

			CPU cpu = new CPU("cpu", 2, 10);
			CpuTest user1 = new CpuTest("user_1", cpu);
			CpuTest user2 = new CpuTest("user_2", cpu);
			// CpuUser user3=new CpuTest("user1");
			// CpuUser user4=new CpuTest("user1");
			//			

			user1.init();

			// Fourth step: Starts the simulation
			GridSim.startGridSimulation();

			logger.info("Finish Simulation");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Unwanted errors happen");
		}
	}

	List<Datum> jobs = new ArrayList<Datum>();
	CPU cpu;

	private void init() {
		Datum job1 = new Datum("j1",900, 10);
		Datum job2 = new Datum("j2",100, 5);
		Datum job3 = new Datum("j3",40, 2);
		Datum job4 = new Datum("j4",10, 5);
		
		

		jobs.add(job1);
		jobs.add(job2);
		jobs.add(job3);
		jobs.add(job4);

		for (Datum job : jobs) {
			job.registerBlocking(this, 112211);
		}

	}

	
	@Override
	public void body() {

		for (Datum j : jobs) {
			for (int i = 0; i < j.size/j.delta; i++) {
				j.deltaList.addLast(j.delta);
			}
			logger.info(">>>>>>>>> submint job " + j + " at time " + GridSim.clock());
			cpu.submint(0, 0, j);
			
			for (int i = 0; i < j.size/j.delta; i++) {
				logger.info("<<<<<<<<< return job "+ j.recieve(this)+ " at time "+ GridSim.clock());
			}
			//logger.info("<<<<<<<<< return job "+ j.recieve(this)+ " at time "+ GridSim.clock());

			// cpu.submint(j);
		}
		// send(cpu.get_id(), 0.0, HTAG.cpu_tic.id());
	}

//	@Override
//	public void jobletComplete(Joblet job) {
//		logger.info("-------------------complete job: " + job + " at time :"
//				+ GridSim.clock());
//
//	}
//
//	
//	@Override
//	public void jobProgress(Joblet job) {
//
//		logger.info("user " + get_name() + " progress: " + job + " at time: "
//				+ GridSim.clock());
//	}
//
//	public static void test1() {
//		List<Joblet> jobs = new ArrayList<Joblet>();
//		Joblet job1 = new Joblet("j_1",50, 10);
//		Joblet job2 = new Joblet("j_2",100, 5);
//		Joblet job3 = new Joblet("j_3",40, 2);
//		Joblet job4 = new Joblet("j_4",10, 5);
//
//		jobs.add(job1);
//		jobs.add(job2);
//		jobs.add(job3);
//		jobs.add(job4);
//
//		Set<Joblet> set = new LinkedHashSet<Joblet>(jobs);
//		logger.info(set);
//		set.remove(job1);
//		logger.info(set);
//	}
}
