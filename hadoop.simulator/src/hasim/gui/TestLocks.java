package hasim.gui;

import org.apache.log4j.Logger;

import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import javax.swing.JFrame;

public class TestLocks extends Thread{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(TestLocks.class);

	static ReentrantLock lock=new ReentrantLock();
	
	
	@Override
	public void run() {
		logger.info("going to lock");
		lock.lock();
		logger.info("locked, going to sleep");
		try {
			sleep(10000);
		} catch (Exception e) {
			// TODO: handle exception
		}finally{
			logger.info("unlock");
			lock.unlock();
		}
		logger.info("stop the thread");
	}
	
	public static void main1(String[] args) {
		TestLocks test=new TestLocks();
		test.start();
		try {
			sleep(1000);
		} catch (Exception e) {
			// TODO: handle exception
		}
		logger.info("going to lock");
		if(lock.isLocked()){
			logger.info("trying to unlock");
			lock.unlock();
			logger.info("suceeded unlocking");
		}
		lock.lock();
		logger.info("main thread get lock");
		try {
			sleep(5000);
		} catch (Exception e) {
			// TODO: handle exception
		}finally{
			logger.info("going to unlock");
			lock.lock();
			logger.info("main thread release lock");
		}
	}
	
	public static void main(String[] args) {
		NewJFrame frame=new NewJFrame();
		frame.setVisible(true);
		
		while(true){
			logger.info("new iteration");
			
			frame.panelWait();
			logger.info("continue after wait");
			
			try {
				sleep(5000);
			} catch (Exception e) {
				// TODO: handle exception
			}
			
		}
	}
}
