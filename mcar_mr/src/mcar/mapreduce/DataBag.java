package mcar.mapreduce;

import java.util.ArrayList;
import java.util.Iterator;

public class DataBag extends ArrayList implements Comparable {

	public DataBag() {
		super();
	}

	public DataBag(DataBag dataBag) {
		super(dataBag);
	}

	public DataBag(int i) {
		super(i);
	}

	@Override
	public int compareTo(Object o) {
		DataBag other=(DataBag)o;
		Iterator i1=this.iterator();
		Iterator i2=other.iterator();
		
		while (i1.hasNext() && i2.hasNext()){
			int dif=((Comparable)i1.next()).compareTo(i2.next());
			if(dif != 0)return dif;
		}
		return (other.size()-this.size());
	}

}
