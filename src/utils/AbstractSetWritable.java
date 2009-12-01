package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;

import com.sun.corba.se.spi.orbutil.fsm.Guard.Result;

public  class AbstractSetWritable<T> extends LinkedHashSet<T>  implements Set<T>{

	/** Cache the hash code for the string */
	int hash; // Default to 0

	private final Comparator<? super T> comparator;

	public Comparator<? super T> comparator() {
		return comparator;
	}

	final int compare(Object k1, Object k2) {
		return comparator==null ? ((Comparable<? super T>)k1).compareTo((T)k2)
				: comparator.compare((T)k1, (T)k2);
	}

	public int compareTo(Object o) {
		if (o == this)
			return 0;
		if (!(o instanceof Set))
			return 0;
		Set<T> s=(Set<T>)o;
		
		int diff=size() - s.size();
		if (diff !=0 )
			return diff;

		Iterator<T> e1 = iterator();
		Iterator<T> e2 = ((AbstractSetWritable<T>) o).iterator();
		while(e1.hasNext() && e2.hasNext()) {
			diff=((Comparable<? super T>)e1.next()).compareTo(e2.next());
			if(diff != 0)
				return diff;
		}
		return 0;
	}


	public AbstractSetWritable(){
		super();
		comparator = null;
	}
	public AbstractSetWritable(int i){
		super(i);
		comparator = null;
	}
	public AbstractSetWritable(Collection<? extends T> c) {
		super(c);
		comparator=null;
	}

	public boolean addAll(Collection<? extends T> c) {
		hash=0;
		return 	super.addAll(c); 
	}




//	public int compareTo(Object o) {
//	MyLinkedSet itm=(MyLinkedSet)o;
//	int sz=size();
//	int diff=sz-itm.size();
//	if (diff !=0)
//	return diff;


//	Iterator<T> iter1=iterator();		
//	Iterator<T> iter2=itm.iterator();

//	for (int i = 0; i < sz;i++) {
//	diff=iter1.next()-iter2.next();
//	if(diff != 0)
//	return diff;
//	}
//	return 0;	

//	if (o == this)
//	return 0;
//	if (!(o instanceof MyLinkedSet))
//	return 0;

//	Iterator<T>  e1 = iterator();
//	Iterator<T> e2 = ((MyLinkedSet<T>) o).iterator();
//	while(e1.hasNext() && e2.hasNext()) {
//	T o1 = e1.next();
//	Object o2 = e2.next();
//	if (!(o1==null ? o2==null : o1.equals(o2)))
//	return false;
//	}
//	return !(e1.hasNext() || e2.hasNext());

//	return -1;
//	}

//	public boolean equals(Object o) {
//	if (o == this)
//	return true;

//	if (!(o instanceof Set))
//	return false;
//	Collection c = (Collection) o;
//	if (c.size() != size())
//	return false;
//	try {
//	return containsAll(c);
//	} catch (ClassCastException unused)   {
//	return false;
//	} catch (NullPointerException unused) {
//	return false;
//	}
//	}


	@Override
	public boolean add(T e) {
		hash=0;
		return super.add(e);
	}

	@Override
	public void clear() {
		hash=0;
		super.clear();
	}

	@Override
	public boolean remove(Object o) {
		hash=0;
		return super.remove(o);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		hash=0;
		return super.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		hash=0;
		return super.retainAll(c);
	}

	public static int genInt(int r){
		return (int)(Math.random()*r);
	}

	public static void testMerge(){
		TreeSet<Integer> ts1=new TreeSet<Integer>();
		TreeSet<Integer> ts2=new TreeSet<Integer>();
		TreeSet<Integer> mrg=new TreeSet<Integer>();
		AbstractSetWritable<Integer> set1;
		AbstractSetWritable<Integer> set2;

		for (int j = 0; j < 1000000; j++) {


			for (int i = 0; i < 5; i++) {
				int rnd=genInt(1000000);
				ts1.add(rnd);
				int rnd2=genInt(1000000);
				ts2.add(rnd2);
			}
			set1=new AbstractSetWritable<Integer>(ts1);
			set2=new AbstractSetWritable<Integer>(ts2);

//			System.out.println("1 ="+ts1);
//			System.out.println("1 ="+set1);
//			System.out.println("2 ="+ts2);
//			System.out.println("2 ="+set2);

			ts1.addAll(ts2);
			Set<Integer> result=set1.merg(set2);

			//System.out.println("mrg1 ="+ts1);
			//System.out.println("mrg2 ="+result);

			Iterator<Integer> iter1=ts1.iterator();
			Iterator<Integer> iter2=result.iterator();
			if( ts1.size() != result.size()){
				System.out.println("different size");
				return;
			}
			while(iter1.hasNext()){
				Integer i1=iter1.next();
				Integer i2=iter2.next();
				//System.out.println("check "+ i1+" "+i2);

				if(! i1.equals(i2)){

					System.out.println("not equal "+ i1+" "+i2);
					return;
				}
			}
			//System.out.println("test "+j +" done");

			ts1.clear();
			ts2.clear();
			set1.clear();
			set2.clear();

		}
		System.out.println("test done");

	}
	public static void testCompareTo(){
		
	}
	public static void main(String[] args) {

		testMerge();
		if(true)return;
		//testMylinked();
		//testMerg();
		List<Integer> lst1=Arrays.asList(new Integer[]{2,5,6,7,10,11,12});
		List<Integer> lst2=Arrays.asList(new Integer[]{2,3,7,11});

		AbstractSetWritable<Integer> set1=new AbstractSetWritable<Integer>(lst1);
		AbstractSetWritable<Integer> set2=new AbstractSetWritable<Integer>(lst2);

		System.out.println(set1.toString());
		System.out.println(set2.toString());


	}

//	private static void testMylinked() {
//	// TODO Auto-generated method stub
//	AbstractSetWritable<Integer> ml=new AbstractSetWritable<Integer>();
//	ml.add(1);
//	ml.add(0);
//	ml.add(2);

//	AbstractSetWritable<Integer> ml2=new AbstractSetWritable<Integer>(ml);

//	ml2.add(1);
//	ml2.add(0);
//	ml2.add(2);
//	ml2.add(3);


//	System.out.println("equal "+ml.equals(ml2));
//	System.out.println("== "+ (ml== ml2));

//	System.out.println("hashcode "+ ml.hashCode()+","+ ml2.hashCode() );

//	HashMap<AbstractSetWritable<Integer>, Integer> map=new HashMap<AbstractSetWritable<Integer>, Integer>();
//	System.out.println("put ml "+map.put(ml, 1));
//	System.out.println("put mls "+map.put(ml2, 2));
//	System.out.println("get "+ map.get(new AbstractSetWritable<Integer>() ));
//	}
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof Set))
			return false;
		Set<T> s=(Set<T>)o;
		if (size() != s.size())
			return false;
		Iterator<T>  e1 = iterator();
		Iterator<T> e2 = s.iterator();
		while(e1.hasNext()) {
			T o1 = e1.next();
			Object o2 = e2.next();
			if (!(o1==null ? o2==null : o1.equals(o2)))
				return false;
		}
		return true;
	}


	public int hashCode() {
		if(hash != 0)
			return hash;
		else{
			int hashCode = 1;
			Iterator<T> i = iterator();
			while (i.hasNext()) {
				T obj = i.next();
				hashCode = 31*hashCode + (obj==null ? 0 : obj.hashCode());
			}
			hash=hashCode;
			return hash;
		}
	}


	public static void testiter(Iterator<Integer> iter){
		System.out.println(iter.next().toString());
		System.out.println(iter.next().toString());
	}

//	abstract public void readFields(DataInput in);// throws IOException {

//	public abstract void write(DataOutput out);

	/**
	 * make sure that sets are not empty
	 * work only with sorted linked hash sets
	 * @return AbstractSetWritable
	 */
	public Set<T> merg(Set<T> other){
		//do not merge with empty sets

		Set<T> result=new LinkedHashSet<T>();
		Iterator<T> iter1=this.iterator();
		Iterator<T> iter2=other.iterator();


		T index1=iter1.next();
		T index2=iter2.next();

		while (true){
			int diff=((Comparable<? super T>)index1).compareTo(index2);
			if( diff < 0 ){
				result.add(index1);
				if(iter1.hasNext()){
					index1=iter1.next();
				}else{
					result.add(index1);
					result.add(index2);
					while(iter2.hasNext())
						result.add(iter2.next());
					break;
				}
			}else if (diff> 0){
				result.add(index2);
				if(iter2.hasNext()){
					index2=iter2.next();
				}else{
					result.add(index2);
					result.add(index1);
					while(iter1.hasNext())
						result.add(iter1.next());
					break;
				}
			}else{
				result.add(index1);
				if(iter1.hasNext()){
					index1=iter1.next();
				}else{
					result.add(index2);
					while(iter2.hasNext())
						result.add(iter2.next());
					break;
				}
				if(iter2.hasNext()){
					index2=iter2.next();
				}else{
					result.add(index1);
					while(iter1.hasNext())
						result.add(iter1.next());

					break;
				}

			}
		}
		return result;
	}

}
