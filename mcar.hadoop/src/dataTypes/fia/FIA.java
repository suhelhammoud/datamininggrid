package dataTypes.fia;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import dataTypes.IntListWritable;

import others.MyConstants;
import others.Tools;

/*abstract public class TestArray {
	abstract public int first();
	abstract public int[] array();
	abstract public void set(int...values);
	abstract public int length();
}*/

/**
 * FixedIntArrayWritable extends by 
  class FIA2 extends FIA{
	private final static int LENGTH=2;
	//private int[] data=new int[LENGTH];
	@Override
	public int size() {
		return LENGTH;
	}
}
 */
abstract public class FIA extends IntListWritable{
	private final static int LENGTH=1;
	
	public static Class<FIA> getClass(int size){
		if(size>10 || size<1){
			System.err.println("FIA"+ size+" class not found");
			return null;
		}
		Class<FIA> result=null;
		try {
			result= (Class<FIA>) Class.forName("dataTypes.fia.FIA"+size);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static FIA getFIAInstance(int size){
		try {
			return (FIA)getClass(size).newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public FIA() {
		super(LENGTH);
	}
	
	abstract public int size();
	
	
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int sz=size();
		clear();
		for (int i = 0; i < sz; i++) {
			add(in.readInt());
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		for (int i = 0,size=size(); i < size; i++) {
			out.writeInt(get(i));
		}
	}
	
	@Override
	public byte[] toBytes(){
		int sz=size()* MyConstants.SIZEOF_INT;
		ByteBuffer bb=ByteBuffer.allocate(sz);
		for (int i : this) {
			bb.putInt(i);
		}
		return bb.array();
	}
	
	
	
	public static List<Integer> listFromBytes(byte[] b){
		int sz=b.length/MyConstants.SIZEOF_INT;
		ByteBuffer bb=ByteBuffer.wrap(b);
		List<Integer> result=new ArrayList<Integer>(sz);
		for (int i = 0; i < sz; i++) {
			result.add(bb.getInt());
		}
		return result;
	}
	

	
	@Override
	public int compareTo(Object o) {
		List<Integer> oData=((List)o);
		int len1=size();
		int diff=len1-oData.size();
		if(diff !=0)return diff;
		for (int i = 0;	i < len1; i++) {
			diff=get(i)-oData.get(i);
			if(diff !=0 )return diff;
		}
		return 0;
	}
	
	private static void testFIA() {
		FIA arr1,arr2,arr3;
		
		arr1= FIA.getFIAInstance(1);
		arr2= FIA.getFIAInstance(2);
		arr3= FIA.getFIAInstance(3);
		
		arr1.setAll(1,11,111);
		arr2.setAll(2,22,222);
		arr3.setAll(3,33,333);
		
		System.out.println("arr1\t"+arr1.size()+"\t"+ arr1.toString());
		System.out.println("arr2\t"+arr2.size()+"\t"+ arr2.toString());
		System.out.println("arr3\t"+arr3.size()+"\t"+ arr3.toString());
		System.out.println(arr1.compareTo(arr3));
		System.out.println();
		System.out.println(""+arr1.toBytes().length);
		System.out.println(""+arr2.toBytes().length);
		System.out.println(""+arr3.toBytes().length);
		
		System.out.println(""+arr1.size());
		System.out.println(""+arr2.size());
	}
	
	public static void main(String[] args) throws Exception{
	
	}
	
	
	
}

