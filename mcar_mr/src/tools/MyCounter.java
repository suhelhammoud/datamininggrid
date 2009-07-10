package tools;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import mcar.mapreduce.*;



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

	 public static DataBag composeRules(DataBag line){
		 DataBag result=new DataBag();

		 TreeMap<DataBag, DataBag> left=new TreeMap<DataBag, DataBag>();
		 TreeMap<DataBag, DataBag> right=new TreeMap<DataBag,DataBag>();

		 for (int i = 0; i < line.size(); i++) {
			 DataBag tmp=new DataBag((DataBag)line.get(i));
			 DataBag coreLeft=new DataBag(tmp);
			 coreLeft.remove(coreLeft.size()-1);
			 DataBag coreRight=new DataBag(tmp);
			 coreRight.remove(0);


			 DataBag leftList=left.get(coreLeft);
			 if(leftList==null)leftList=new DataBag();
			 leftList.add(tmp);
			 left.put(coreLeft, leftList);

			 DataBag rightList=right.get(coreRight);
			 if(rightList==null)rightList=new DataBag();
			 rightList.add(tmp);
			 right.put(coreRight, rightList);
		 }
//		 System.out.println("left "+left);
//		 System.out.println("right "+right);

		 left.remove(left.firstKey());
		 right.remove(right.lastKey());

		 for (Map.Entry<DataBag, DataBag> e : right.entrySet()) {

			 DataBag start=e.getValue();

			 DataBag stop=left.get(e.getKey());
			 if(stop==null)continue;


			 //				System.out.println("start "+ start);
			 //				System.out.println("stop "+ stop);
			 for (Object o1 : start) {
				 DataBag is1=(DataBag)o1;
				 for (Object o2 : stop) {
					 DataBag is2=(DataBag)o2;
					 DataBag compound = new DataBag(is1);
					 compound.add(is2.get(is2.size()-1));
					 result.add(compound);
//					 System.out.println("join "+ is1+"\t"+is2
//							 +"=\t"+ compound);
				 }
			 }
		 }
		 return result;

	 }
	 
	 public static DataBag compose(DataBag line){
		 int sz=((DataBag)line.get(0)).size();
		 DataBag result=new DataBag();

		 TreeMap<DataBag, DataBag> left=new TreeMap<DataBag, DataBag>();
		 TreeMap<DataBag, DataBag> right=new TreeMap<DataBag,DataBag>();

		 for (int i = 0; i < line.size(); i++) {
			 DataBag tmp=new DataBag((DataBag)line.get(i));
			 tmp.remove(sz-1);
			 
			 DataBag coreLeft=new DataBag(tmp);
			 coreLeft.remove(coreLeft.size()-1);
			 DataBag coreRight=new DataBag(tmp);
			 coreRight.remove(0);


			 DataBag leftList=left.get(coreLeft);
			 if(leftList==null)leftList=new DataBag();
			 leftList.add(line.get(i));
			 left.put(coreLeft, leftList);

			 DataBag rightList=right.get(coreRight);
			 if(rightList==null)rightList=new DataBag();
			 rightList.add(line.get(i));
			 right.put(coreRight, rightList);
		 }
		 System.out.println("left "+left);
		 System.out.println("right "+right);

		 left.remove(left.firstKey());
		 right.remove(right.lastKey());

		 for (Map.Entry<DataBag, DataBag> e : right.entrySet()) {

			 DataBag start=e.getValue();

			 DataBag stop=left.get(e.getKey());
			 if(stop==null)continue;


			 //				System.out.println("start "+ start);
			 //				System.out.println("stop "+ stop);
			 for (Object o1 : start) {
				 DataBag is1=(DataBag)o1;
				 for (Object o2 : stop) {
					 DataBag is2=(DataBag)o2;
					 DataBag compound = new DataBag(is1.size()+2);
					 for (int i = 0; i < is1.size()-2; i++) {
						compound.add(is1.get(i));
					}
					 compound.add(is2.get(is2.size()-2));
					 compound.add(is1.size()-1);
					 compound.add(is2.size()-1);
					 result.add(compound);
//					 System.out.println("join "+ is1+"\t"+is2
//							 +"=\t"+ compound);
				 }
			 }
		 }
		 return result;

	 }
	 public static void main(String[] args) {

		 List<Integer> list=Arrays.asList(new Integer[]{1,2,3,4,5,6});

		 MyCounter m=new MyCounter(6,4);
		 System.out.println(m.length());
		 List<int[]> all=new ArrayList<int[]>();
		 while (m.hasNext()){
			 all.add(m.nextIndex().clone());
		 }
		 DataBag db=new DataBag();

		 for (int[] is : all) {
			 DataBag d=new DataBag();
			 for (Integer i : is) {
				d.add(i);
			}
			 db.add(d);
		 }
		 

		 TreeMap<MyList, MyList> left=new TreeMap<MyList, MyList>();
		 TreeMap<MyList, MyList> right=new TreeMap<MyList, MyList>();

		 for (int[] is : all) {
			 MyList tmp=new MyList();
			 for (int i : is)tmp.add(i);
			 MyList coreLeft=new MyList(tmp);
			 coreLeft.remove(coreLeft.size()-1);
			 MyList coreRight=new MyList(tmp);
			 coreRight.remove(0);


			 MyList leftList=left.get(coreLeft);
			 if(leftList==null)leftList=new MyList();
			 leftList.add(tmp);
			 left.put(coreLeft, leftList);

			 MyList rightList=right.get(coreRight);
			 if(rightList==null)rightList=new MyList();
			 rightList.add(tmp);
			 right.put(coreRight, rightList);

		 }

		 System.out.println("left "+left);
		 System.out.println("right "+right);

		 left.remove(left.firstKey());
		 right.remove(right.lastKey());

		 for (Map.Entry<MyList, MyList> e : right.entrySet()) {

			 MyList start=e.getValue();

			 MyList stop=left.get(e.getKey());
			 if(stop==null)continue;


			 //			System.out.println("start "+ start);
			 //			System.out.println("stop "+ stop);
			 for (Object o1 : start) {
				 MyList is1=(MyList)o1;
				 for (Object o2 : stop) {
					 MyList is2=(MyList)o2;
					 MyList compound = new MyList(is1);
					 compound.add(is2.get(is2.size()-1));
					 System.out.println("join "+ is1+"\t"+is2
							 +"=\t"+ compound);
				 }
			 }
		 }
		 
		 System.out.println("-------------");
		 DataBag out=composeRules(db);
		
		 System.out.println("*********");
		for (Object object : out) {
			System.out.println(object.toString());
		}
		System.out.println("yyyyyyyyyyyyyyyyyyyyyyyyyy");
		DataBag out2=compose(db);
		for (Object object : out2) {
			System.out.println(object);
		}

		 //		List<Integer> lst1=Arrays.asList(new Integer[]{1,2});
		 //		List<Integer> lst2=Arrays.asList(new Integer[]{3,4});
		 //		List<Integer> lst3=Arrays.asList(new Integer[]{5,6});
		 //		
		 //		List[] all={lst1,lst2,lst3};
		 //		
		 //
		 //		MyCounter<List<Integer>> my=new MyCounter<List<Integer>>(all,2);

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

class MyList extends ArrayList implements Comparable{
	public MyList(MyList tmp) {
		for (Object o : tmp) {
			add(o);
		}
	}
	public MyList() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compareTo(Object o) {
		List<Integer> list=(List<Integer>)o;
		for (int i = 0; i < size() && i< list.size(); i++) {
			int dif=((Comparable)get(i)).compareTo(list.get(i));
			if (dif != 0)return dif;
		}
		return list.size()-size();
	}

}
