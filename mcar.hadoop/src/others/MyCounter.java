package others;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.sun.corba.se.impl.javax.rmi.CORBA.Util;

public class MyCounter<E> implements Iterator<E> {
	final int size;
	final int len;

	int[] start;
	int[] stop;
	int[] index;
	int pos;
	// final E[] sum; //for Long in BigInteger
	E[] items;
	private  Class<E> claz;

	protected final int defaultPos;

	protected final long length;
	protected int counter = 0;

	public static long length(int size, int len) {
		List<Integer> up = new ArrayList<Integer>(len);
		List<Integer> down = new ArrayList<Integer>(len);

		BigInteger result = BigInteger.ONE;
		for (int i = size - len + 1; i <= size; i++)
			up.add(i);
		for (int i = 1; i <= len; i++)
			down.add(i);

		for (int i = 0; i < up.size(); i++) {
			result = result.multiply(BigInteger.valueOf(up.get(i)));
		}
		for (int i = 0; i < up.size(); i++) {
			result = result.divide(BigInteger.valueOf(down.get(i)));
		}
		return result.longValue();
	}

	public long length() {
		return length(size, len);
	}

	public MyCounter(E[] items, int len){
		this(items.length,len);
		this.items=items;
		claz=(Class<E>) items[0].getClass();
	}
	public MyCounter(int size, int len) {
		this.size = size;
		this.len = len;

		this.start = new int[len];
		for (int i = 0; i < start.length; i++)
			start[i] = i;

		this.stop = new int[len];
		stop[0] = size - len;
		for (int i = 1; i < stop.length; i++)
			stop[i] = stop[i - 1] + 1;

		index = new int[len];
		for (int i = 1; i < index.length; i++)
			index[i] = start[i];

		index[index.length - 1] = index[index.length - 1] - 1;
		defaultPos = len - 1;
		pos = defaultPos;
		length = length(size, len);

		/*
		 * //for Long and Big Ingteger sum=new BigInteger[len+1];
		 * sum[0]=BigInteger.valueOf(0); for (int i = 0; i < sum.length-1; i++)
		 * { sum[i+1]=BigInteger.ONE.shiftLeft(index[i]).or(sum[i]);
		 * //sum[i+1]=1L << index[i] | sum[i]; }
		 */
	}

	/*
	 * private void startCode() {
	 * 
	 * for (int i = 0,iii=size-len; i<=iii; i++) { int ri=1<<i; for (int j =
	 * i+1, jjj=iii+1; j <= jjj; j++) { int rj=(1<<j | ri);
	 * 
	 * for (int k = j+1, kkk=jjj+1; k <= kkk; k++) { int rk=1<<k | rj;
	 * 
	 * //print the result
	 * System.out.println(rk+"\t"+format(rk)+"\t"+Integer.bitCount(rk)+
	 * "\t"+i+" "+j+" "+k); }
	 * 
	 * }
	 * 
	 * }
	 * 
	 * }
	 */public boolean hasNext() {
		return counter < length;
	}

	@SuppressWarnings("unchecked")
	public E[] nextItems(){
		counter++;
		advance(defaultPos);
	    
		E[] result=(E[])Array.newInstance(claz,len);
		for (int i = 0; i < result.length; i++) {
			result[i]=items[index[i]];
		}
		return result;
	}
	
	public int[] nextIndex() {
		counter++;
		advance(defaultPos);
		int[] copy = new int[index.length];
		System.arraycopy(index, 0, copy, 0, index.length);
		return copy;
	}

	public void remove() {
	}

	public long nextLong() {
		counter++;
		advance(defaultPos);

		long result = 0;
		for (int i : index) {
			result = 1L << i | result;
		}
		return result;
	};

	public BigInteger nextBigIngeger() {
		counter++;
		advance(defaultPos);

		BigInteger result = BigInteger.ZERO;
		for (int i : index) {
			result = BigInteger.ONE.shiftLeft(i).or(result);
		}
		return result;
	};

	protected void advance(int p) {
		if (p == -1)
			return;
		index[p]++;

		if (index[p] > stop[p]) {
			if (p == 0)
				return;
			advance(p - 1);
			start[p] = index[p - 1] + 1;
			index[p] = start[p];
		}
	}


	public static String format(long i) {
		return Integer.toBinaryString((int) i - 1 - Integer.MAX_VALUE);
	}

	@Override
	public E next() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static void main(String[] args) {
		List<Integer> lst1=Arrays.asList(new Integer[]{1,2});
		List<Integer> lst2=Arrays.asList(new Integer[]{3,4});
		List<Integer> lst3=Arrays.asList(new Integer[]{5,6});
		
		List[] all={lst1,lst2,lst3};
		

		MyCounter<List<Integer>> my=new MyCounter<List<Integer>>(all,2);
		while(my.hasNext())
			System.out.println(Tools.join(my.nextItems(), ","));
		
	}

}

class MyCounterBig extends MyCounter<BigInteger> {

	BigInteger[] sum;

	public MyCounterBig(int size, int len) {
		super(size, len);

		sum = new BigInteger[len + 1];
		sum[0] = BigInteger.valueOf(0);
		for (int i = 0; i < sum.length - 1; i++) {
			sum[i + 1] = BigInteger.ONE.shiftLeft(index[i]).or(sum[i]);
			// sum[i+1]=1L << index[i] | sum[i];
		}
	}

	@Override
	protected void advance(int p) {
		if (p == -1)
			return;

		index[p]++;
		sum[p + 1] = BigInteger.ONE.shiftLeft(index[p]).or(sum[p]);
		// sum[p+1] = 1L << index[p] | sum[p];

		if (index[p] >= stop[p]) {
			if (p == 0)
				return;
			start[p] = index[p - 1] + 1;
			index[p] = start[p];
			advance(p - 1);
		}
	}

	@Override
	public BigInteger next() {
		counter++;
		advance(defaultPos);
		return sum[len];
	}

	@Override
	public long nextLong() {
		return next().longValue();
	}

	@Override
	public int[] nextIndex() {
		return null;
	}


}

class MyCounterLong extends MyCounter<Long> {

	long[] sum;

	public MyCounterLong(int size, int len) {
		super(size, len);

		sum = new long[len + 1];
		sum[0] = 0L;
		for (int i = 0; i < sum.length - 1; i++) {
			sum[i + 1] = 1L << index[i] | sum[i];
		}
	}

	@Override
	protected void advance(int p) {
		if (p == -1)
			return;

		index[p]++;
		sum[p + 1] = 1L << index[p] | sum[p];

		if (index[p] >= stop[p]) {
			if (p == 0)
				return;
			start[p] = index[p - 1] + 1;
			index[p] = start[p];
			advance(p - 1);
		}
	}

	@Override
	public Long next() {
		counter++;
		advance(defaultPos);
		return sum[len];
	}

	@Override
	public long nextLong() {
		return next();
	}

	@Override
	public int[] nextIndex() {
		return null;
	}

}

class MyCounterLongRecursive {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger
			.getLogger(MyCounterLongRecursive.class);
	final int size;
	final int len;
	long[] arr;
	int arrIndex = 0;

	public MyCounterLongRecursive(int size, int len) {

		this.size = size;
		this.len = len;
		this.arrIndex = 0;
	}

	public static long[] getSetsArray(int size, int len) {
		MyCounterLongRecursive t = new MyCounterLongRecursive(size, len);
		int sz = (int) MyCounter.length(size, len);
		t.arr = new long[sz];
		t.recvArray(0, 0, size - len, 0);
		return t.arr;
	}

	public void recvArray(int loop, int start, int stop, long r) {
		if (loop >= len)
			arr[arrIndex++] = r;
		else
			for (int i = start; i <= stop; i++)
				recvArray(loop + 1, i + 1, stop + 1, 1 << i | r);
	}

}
