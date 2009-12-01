package hasim;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

public class Netlet extends Datum{
	
	
	
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Netlet.class);

	private int from,to;
	
	public int getFrom() {
		return from;
	}


	public void setFrom(int from) {
		this.from = from;
	}


	public int getTo() {
		return to;
	}


	public void setTo(int to) {
		this.to = to;
	}

	Set<NetletUser> users=new LinkedHashSet<NetletUser>();

	
	public Netlet(String name, double size, double delta, double entries) {
		super(name, size, delta, entries);
	}
	public Netlet(Datum data){
		super(data);
	}


	public boolean registerUser(NetletUser user){
		return users.add(user);
	}
	public boolean unregisterUser(NetletUser user){
		return users.remove(user);
	}
	
	

	
	
	

	public void notifyUsers(){
		for (NetletUser user : users) {
			user.netletProgress(this);
		}
		for (NetletUser user : users) {
			if( percent() >=  1)
				user.netletComplete(this);
		}
	}

	
}