package hasim.gui;


import org.apache.log4j.Logger;
import hasim.gui.HMonitor;
import hasim.gui.HMonitor.DebugMode;

import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.swing.DropMode;
import javax.swing.SwingUtilities;

import gridsim.GridSim;
import hasim.HJobTracker;
import hasim.NetEnd;

public class Workbench extends GridSim{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Workbench.class);

	public static HJobTracker jobtracker;
	public static Map<String, HMonitor> monitors=new LinkedHashMap<String, HMonitor>();

	public Workbench(String name) throws Exception {
		super(name);
	}

	public static void initGridSim(){
		Calendar calendar = Calendar.getInstance();
		boolean trace_flag = true;
		// Initialize the GridSim package
		System.out.println("Initializing GridSim package");
		GridSim.init(5, calendar, trace_flag);

	}

	public static void startGridSim(){
		GridSim.startGridSimulation();
		logger.info("end simulation");
	}

	public  static void initSimulator() throws Exception {

		jobtracker=new HJobTracker("jobTracker");

		jobtracker.initSimulation(null, null);

		HMonitor.debugMode=DebugMode.SLEEP_STEP;
		HMonitor.SLEEP=700;

	}



	


	public static void main(String[] args) {
		System.out.println("start");
		try {
			Workbench.initGridSim();

			Workbench.initSimulator();
			jobtracker.monitor.setVisible(true);
			
			Main inst = new Main();
			inst.setVisible(true);
			
			inst.jobTracker=jobtracker;
			
			jobtracker.simoTree=inst.simoPanel;
			
			Workbench.startGridSim();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	

	@Override
	public void body() {
		logger.info("start workbench");
		//jobtracker.startSimulation();
	}
}
