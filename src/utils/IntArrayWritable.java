package utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import javax.imageio.stream.FileImageInputStream;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {
	public IntArrayWritable() { 
		super(IntWritable.class); 
	}
	public IntArrayWritable(List<Integer> lst) { 
		super(IntWritable.class);
		IntWritable[] arr=new IntWritable[lst.size()];
		for (int i = 0; i < arr.length; i++) {
			arr[i]=new IntWritable(lst.get(i));
		}
		set(arr);
	}
	public IntArrayWritable(TreeSet<Integer> tree) { 
		super(IntWritable.class);
		IntWritable[] arr=new IntWritable[tree.size()];
		
		Iterator<Integer> iter=tree.iterator();
		for (int i = 0; i < arr.length; i++) {
			arr[i]=new IntWritable(iter.next());
		}
		set(arr);
	}
	public IntArrayWritable(String[] arrStr) { 
		super(IntWritable.class);
		IntWritable[] intArr=new IntWritable[arrStr.length];
		for (int i = 0; i < intArr.length; i++) {
			intArr[i]=new IntWritable(Integer.valueOf(arrStr[i]));
		}
		set(intArr);
	}
	
	public static void main(String[] args){
		String[] iarr={"1","2","3","4"};
		IntWritable[] iws=new IntWritable[iarr.length];
		IntArrayWritable arr=new IntArrayWritable(iarr);
		System.out.println(MapTools.join("-",arr.toStrings()));
		
		try {
			DataOutputStream out = new DataOutputStream(new FileOutputStream("out.txt"));
			arr.write(out);
			out.close();
			
			DataInputStream in = new DataInputStream(new FileInputStream("out.txt"));
			IntArrayWritable a=new IntArrayWritable();
			a.readFields(in);
			System.out.println(MapTools.join("^",arr.toStrings()));			
			in.close();
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}

	/**
	 * needed this method to show data in the output
	 */
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return MapTools.join(" ", toStrings());
	}
}