package hasim;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

import gridsim.GridSim;
import gridsim.GridSimTags;

import java.util.LinkedHashSet;
import java.util.Set;

public class HBuffer extends GridSim implements HBufferUser{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HBuffer.class);

	public static void main(String[] args) throws Exception {

		HBuffer buffer=new HBuffer("buffer" , 120,100,20, 0.81);
		HBufferTest test=new HBufferTest();
		buffer.regiseterUser(test);
		logger.info("registerd users "+ buffer.users.size() );
		logger.info(buffer);

		for (int i = 0; i < 200; i++) {
			buffer.add(1);
			
		}
		logger.info(buffer);
	}
	
	final String name;
	final double maxSize;
	final double size;
	final double deltaThreshold;
	
	final double threshold;
	double currentSize;
	
	double lastNotifySize;
	private boolean doneDelta=false;
	private boolean doneThreshold=false;
	
	
	private boolean doneFull=false;
	
	
	
	Set<HBufferUser> users=new LinkedHashSet<HBufferUser>();
	HMapper mapper;
	HSpiller spiller;
	
	public HBuffer(String name,double size) throws Exception{
		super(name);
		this.name=name;
		this.maxSize=size;
		this.size=size;
		this.threshold=0.8;
		this.deltaThreshold=0.0;
	}
	public HBuffer(String name,double maxSize,double size,
			double deltaThreshold, double thresholdPercent) throws Exception {
		super(name);
		this.name=name;
		this.maxSize=maxSize;
		this.size=size;
		this.threshold=thresholdPercent * size;
		this.deltaThreshold=deltaThreshold;
		
	}

	public boolean add(double data){
		if(currentSize+data > maxSize ) return false;
		currentSize+=data;
		
		notifyUsers();
		return true;
	}
	

	
	@Override
	public void body() {
		while(Sim_system.running()){
			Sim_event ev=new Sim_event();
			sim_get_next(ev);
			
			int tag=ev.get_tag();
			
			if(tag== GridSimTags.END_OF_SIMULATION){
				logger.debug("End of simulation");
				break;
			}
			
			if(tag == HTAG.mem_add.id()){
				add((Double)ev.get_data());
				if(percent()< 1.0)
					send(ev.get_src(), 0.0, HTAG.mem_add_return.id(), HTAG.mem_add_return);

				continue;
			}
			
			if( tag == HTAG.mem_flush.id()){
				Sim_event e2=new Sim_event();
				//wait untill running spill is finished
				if (spiller.isRunning()){
					sim_get_next(HTAG.mem_reset.predicate(), ev);
					reset();
				}
				send(spiller.get_id(), 0.0, HTAG.spiller_start.id(), this);
			}
			
			if(tag == HTAG.mem_reset.id()){
				reset();
				send(mapper.get_id(), 0.0, HTAG.mapper_unblock.id());
			}
			
			if( tag == HTAG.mem_set_mapper.id()){
				this.mapper=(HMapper)ev.get_data();
				break;
			}
		}
	}
	
	public HMapper getMapper() {
		return mapper;
	}
	public HSpiller getSpiller(){
		return spiller;
	}
	public boolean isDoneDelta() {
		return doneDelta;
	}
	public boolean isDoneFull() {
		return doneFull;
	}
	public boolean isDoneThreshold() {
		return doneThreshold;
	}
	private void notifyUsers(){
		if((currentSize- lastNotifySize) >= deltaThreshold){
			for (HBufferUser user : users) {
				user.bufferDelta(this);
			}
			lastNotifySize=currentSize;
		}
		if(currentSize >= threshold && ! doneThreshold){
			doneThreshold=true;
			for (HBufferUser user : users) {
				user.bufferThreshold(this);
			}
		}
		if( currentSize>= size && ! doneFull){
			doneFull =true;
			doneDelta=true;
			for (HBufferUser user : users) {
				user.bufferFull(this);
			}
		}		
	}
	
	public void receiveAddReturn(Sim_entity etty){
		Sim_event ev=new Sim_event();
		etty.sim_get_next(HTAG.mem_add_return.predicate(),ev);
		return;
	}
	
	public double percent(){
		return currentSize/size;
	}
	public boolean regiseterUser(HBufferUser user){
		return users.add(user);
	}
	
	public void reset(){
		this.currentSize=0;
		this.lastNotifySize=0;
		this.doneThreshold=false;
		this.doneDelta=false;
		this.doneFull=false;
	}
	
	/**
	 * make sure to set the mapper and the spiller
	 *  berfore the start of the simulation
	 * @param mapper
	 * @param spiller
	 */
	public void setMapperSpiller(HMapper mapper, HSpiller spiller) {
		this.mapper = mapper;
		this.spiller=spiller;
	}
	public boolean subtract(double data){
		if(currentSize-data < 0) return false;
		currentSize -= data;
		
		doneThreshold= currentSize > threshold;
		doneFull= currentSize > size;
		lastNotifySize=currentSize;

		return true;
	}
	
	@Override
	public String toString() {
		StringBuffer sb= new StringBuffer(name);
		sb.append("\tsize:"+size+"\tcSize:"+currentSize+ "\tpercent:"+percent());
		return sb.toString();
	}
	public boolean unRegisterUser(HBufferUser user){
		return users.remove(user);
	}
	@Override
	public void bufferDelta(HBuffer b) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void bufferFull(HBuffer b) {
		send(mapper.get_id(), 0.0, HTAG.mapper_block.id());		
	}
	
	@Override
	public void bufferThreshold(HBuffer b) {
		send(spiller.get_id(), 0.0, HTAG.spiller_start.id(), this);
	}

}

class HBufferTest implements HBufferUser{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HBufferTest.class);

	
	@Override
	public void bufferDelta(HBuffer b) {
		logger.debug(b);
		
	}

	@Override
	public void bufferFull(HBuffer b) {
		logger.debug(b);
		
	}

	@Override
	public void bufferThreshold(HBuffer b) {
		logger.debug(b);
		
	}
	
}

interface HBufferUser{
	public void bufferDelta(HBuffer b);
	public void bufferFull(HBuffer b);
	public void bufferThreshold(HBuffer b);
}