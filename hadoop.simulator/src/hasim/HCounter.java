package hasim;

import org.apache.log4j.Logger;

import java.util.LinkedHashMap;


@SuppressWarnings("serial")
public class HCounter extends LinkedHashMap<CounterTag, Double>{
	/**
 * Logger for this class
 */
private static final Logger logger = Logger.getLogger(HCounter.class);
	
	public HCounter() {
		for (CounterTag tag : CounterTag.values()) {
			this.put(tag, 0.0);
		}
	}
	
	//TODO need to synchronize
	public double inc(CounterTag tag, double value){
		double cValeu=get(tag);
		return value+ put(tag, cValeu+ value);
	}
	
	public static void main(String[] args) {
		HCounter c=new HCounter();
		logger.info(c.inc(CounterTag.COMBINE_INPUT_RECORDS, 1));
		logger.info(c.inc(CounterTag.COMBINE_INPUT_RECORDS, 1.5));
		logger.info(c.inc(CounterTag.COMBINE_INPUT_RECORDS, 12));
	}
	
}
