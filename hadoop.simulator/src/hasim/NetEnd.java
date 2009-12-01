package hasim;

import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.IO_data;
import gridsim.ParameterException;
import gridsim.net.FIFOScheduler;
import gridsim.net.InfoPacket;
import gridsim.net.Link;
import gridsim.net.RIPRouter;
import gridsim.net.Router;
import gridsim.net.SimpleLink;
import hasim.json.JsonConfig;
import hasim.json.JsonMachine;
import hasim.json.JsonRealRack;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;


import org.apache.log4j.Logger;


import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

public class NetEnd extends GridSim{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(NetEnd.class);

	//public static double delay=0.1;


	public static Set<Link> links=new LinkedHashSet<Link>();
	public static Set<Router> routers=new LinkedHashSet<Router>();

	public static Map<String, NetEnd> netends=new LinkedHashMap<String, NetEnd>();

	public NetEnd(String name, double baud) throws Exception {
		super(name, new SimpleLink("link_"+name, baud, 
				0.0, 150));

	}
	public NetEnd(String name, Link link) throws Exception {
		super(name, link);

	}
	
	public static void stopSimulation(){
		logger.info("Stopping NetEnd Simulation");
		for (NetEnd netend : netends.values()) {
			netend.send(netend.get_id(), 0.0, GridSimTags.END_OF_SIMULATION);
		}
	}


	public static void init( JsonRealRack rack) throws Exception{
		assert rack != null;
		Router router=new RIPRouter(rack.getRouter(),false);
		routers.add(router);
		
		for (JsonMachine  m : rack.getMachines()) {
			SimpleLink link=new SimpleLink("link_"+m.getHostName(),m.getBaudRate(),
					HJobTracker.config.getPropDelay(), 150);//HJobTracker.config.getMaxIM());
			
			links.add(link);
			
			NetEnd netend=new NetEnd("netend_"+m.getHostName(), link);
			netends.put(m.getHostName(), netend);

			FIFOScheduler userSched = new FIFOScheduler("sched_"+m.getHostName());
			router.attachHost(netend, userSched);
		}
	}

	public static void init( List<String> sEnds, double baudRate, int maxIM){
		if( sEnds==null)return;

		Router r1 = new RIPRouter("r1", false);   // router 1

		routers.add(r1);
		
		
		for (String entName : sEnds) {
			logger.debug("create entity:"+ entName);
			try {
				SimpleLink link=new SimpleLink("link_"+entName, baudRate,1.0, maxIM);
				links.add(link);

				NetEnd netend=new NetEnd(entName, link);
				netends.put(entName, netend);

				FIFOScheduler userSched = new FIFOScheduler("sched_"+entName);
				r1.attachHost(netend, userSched);

			} catch (NullPointerException e) {
				e.printStackTrace();
			} catch (ParameterException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void body() {
		gridSimHold(1000);
		//send(get_id(), 100.00, HTAG.heartbeat.id());

		while(Sim_system.running()){
			
			
			
			Sim_event ev=new Sim_event();

			sim_get_next(ev);
			int tag = ev.get_tag();

			logger.debug("receive event "+ HTAG.get(ev.get_tag())+" tag="+ ev.get_tag());
			if (tag == HTAG.heartbeat.id()) {

				send(get_id(), 100.00, HTAG.heartbeat.id());
			}



			if(tag == GridSimTags.END_OF_SIMULATION){
				logger.info(get_name()+" end simulation at time "+ GridSim.clock());
				break;
			}

			if(tag == HTAG.netlet_send.id()){
				logger.debug("receive "+ ev.get_data()+ " at time : "+GridSim.clock());
				//IO_data data=(IO_data)ev.get_data();
				
				Object o=ev.get_data();
				

				if( !(o instanceof Datum)){
					logger.error("object received is not instance of Datum");
					continue;
				}

				Datum netlet=(Datum)o;

				Set<Sim_entity> users=netlet.getBlockingUsers();
				for (Sim_entity user : netlet.getBlockingUsers()) {
					send(user.get_id(), 0.0, HTAG.netlet_send_return.id(), netlet);
				}

				logger.debug("receive netlet "+ netlet+ " at time "+ GridSim.clock());
				continue;
			}

			logger.info(" another tag received "+ HTAG.get(tag));

		}
		
		shutdownUserEntity();
		terminateIOEntities();
	}
	
	
	public void body2() {
		//
		//		Sim_event ev=new Sim_event();
		//		while(Sim_system.running()){
		//			sim_get_next(ev);
		//
		//			int tag = ev.get_tag();
		//
		//			if(tag == GridSimTags.END_OF_SIMULATION)break;
		//
		//			if(tag == HTAG.netlet_send.id()){
		//				Netlet netlet=(Netlet)ev.get_data();
		//				double sum=0;
		//				//TODO make sure it is working
		//				while(sum < netlet.size){
		//					//sendData(, ntlt)(netlet, delay++);
		//					sum+=netlet.delta;
		//				}
		//				continue;
		//			}
		//
		//			if( tag == NetUser.SEND_MSG){
		//				Netlet netlet=(Netlet)ev.get_data();
		//				netlet.currentSize += netlet.delta;
		//				netlet.notifyUsers();
		//
		//				//added for blocking users
		//
		//				Set<Sim_entity> blockingUsers=netlet.getBlockingUsers();
		//
		//				for (Iterator<Sim_entity> iter = blockingUsers.iterator(); iter
		//				.hasNext();) {
		//					Sim_entity user = iter.next();
		//					send(user.get_id(), 0.0, HTAG.netlet_part_return.id(),netlet);
		//					if(netlet.percent()>=1)
		//						send(user.get_id(), 0.0, HTAG.netlet_complete_return.id(),netlet);
		//
		//					//iter.remove();//TODO not working
		//
		//				}
		//				//end blocking notification
		//				continue;
		//
		//			}
		//
		//		}
		//		 // shut down I/O ports
		//        shutdownUserEntity();
		//        terminateIOEntities();

	}

	//	public boolean sendData(String to, Netlet ntlt){
	//		return sendData(to, ntlt.size,ntlt);
	//	}

	public boolean sendData(String to, double size, Object o){
		return sendData(to, size, o, 0);
	}

	public boolean sendData(String to, double size, Object o, double delay){
		NetEnd dist=netends.get(to);
		if(dist==null)return false;
		if(o ==null || size==0 ){
			logger.error("wrong values object and size ");
			return false;
		}
			
		IO_data data=new IO_data(o, (long)size , dist.get_id());
		super.send(super.output, delay, HTAG.netlet_send.id(),data);
		return true;
	}

	public static boolean sendData(String from, String to, double size, Object o, double delay){
		NetEnd ntFrom=netends.get(from);
		NetEnd ntTo=netends.get(to);
		if(ntFrom==null || ntTo == null){
			logger.error("No netends with the name:"+ from +","+to+" in the system");
			return false;
		}
		return ntFrom.sendData(to, size, o, delay);
	}



	public static boolean sendData(String from, String to, double size, Object o){
		return sendData(from, to, size, o,0.0);
	}


	public static boolean sendData(String from, String to, Netlet ntlt){
		return sendData(from, to, ntlt.size, ntlt);
	}
	
	public static void main(String[] args) throws Exception {
		
		Calendar calendar = Calendar.getInstance();

        // a flag that denotes whether to trace GridSim events or not.
        boolean trace_flag = true;

        // Initialize the GridSim package
        System.out.println("Initializing GridSim package");
        GridSim.init(1, calendar, trace_flag);
         
        double baud_rate = 100; // bits/sec
        double propDelay = 10;   // propagation delay in millisecond
        int mtu = 150;          // max. transmission unit in byte
        int i = 0;
        
        String[] ends={"a","b","c","d","e"};
        NetEnd.init(Arrays.asList(ends), baud_rate, mtu);
        
        NetEndTest test=new NetEndTest("test");
        
        GridSim.startGridSimulation();
        logger.info("end simulation");

		
	}
	
	@Override
	public String toString() {
		return "netend:"+getLink().getBaudRate();
	}

}

class NetEndTest extends GridSim{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(NetEndTest.class);

	public NetEndTest(String name) throws Exception {
		super(name);
	}
	
	@Override
	public void body() {
		
		Datum n1=new Datum("n1", 10000, 100, 100);
		Datum n2=new Datum("n2", 30, 100, 100);
		Datum n3=new Datum("n3", 600, 100, 100);

		n1.registerBlocking(this, 7777);
		n2.registerBlocking(this, 7777);
		n3.registerBlocking(this, 7777);

		NetEnd.sendData("a", "c",	10000, "msg", 1.0);
//		
        super.gridSimHold(6.0);

//		logger.info("semd n1 at time "+GridSim.clock());
//		NetEnd.sendData("a", "b", n1);
		
		
//		
//		logger.info("semd n2 at time "+GridSim.clock());
//		NetEnd.sendData("a", "b", n2);
//		
//		logger.info("semd n3 at time "+GridSim.clock());
//		NetEnd.sendData("c", "b", n3);
//		
//		logger.debug(" send all netlets");
		
//		Sim_event ev=new Sim_event();
//		sim_get_next(ev);
		
		
		
		
//		shutdownUserEntity();
//		terminateIOEntities();

	}
	
}
