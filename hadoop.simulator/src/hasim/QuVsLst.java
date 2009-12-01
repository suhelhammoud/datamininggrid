package hasim;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QuVsLst {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(QuVsLst.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Random r=new Random();
		int sz=100000;
		List<Double> data=new ArrayList<Double>(sz);
		
		for (int i = 0; i < sz; i++) {
			data.add(r.nextDouble());
		}
		
		
		LinkedList<Double> list=new LinkedList<Double>();
		Queue<Double> q1=new LinkedBlockingQueue<Double>();
		
		
		long t;
		t=tic();
		for (int i = 0; i < sz; i++) {
			q1.offer(data.get(i));
		}
		logger.info("que adding "+ toc(t));


		
		
		t=tic();
		for (int i = 0; i < sz; i++) {
			list.addLast(data.get(i));
		}
		logger.info("list adding "+ toc(t));
		
		t=tic();
		for (int i = 0; i < sz; i++) {
			q1.remove();
		}
		logger.info("que  removing "+ toc(t));


		
		
		t=tic();
		for (int i = 0; i < sz; i++) {
			list.removeFirst();
		}
		logger.info("list removing "+ toc(t));
		
	}

	public static long tic(){
		return System.nanoTime();
	}
	public static long toc(long t){
		return System.nanoTime()-t;
	}
}
