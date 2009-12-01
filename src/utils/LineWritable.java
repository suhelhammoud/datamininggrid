package utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import javax.imageio.stream.FileImageInputStream;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class LineWritable extends ArrayWritable {
	public LineWritable() { 
		super(IntArrayWritable.class); 
	}
	
	public static void main(String[] args){
		int[] iarr1={1,2,3,4};
		int[] iarr2={11,22,33,44};
		IntWritable[] iws0=new IntWritable[iarr1.length];
		IntWritable[] iws1=new IntWritable[iarr2.length];
		
		IntArrayWritable arr0=new IntArrayWritable();
		arr0.set(iws0);
		IntArrayWritable arr1=new IntArrayWritable();
		arr1.set(iws1);

		IntArrayWritable[] data=new IntArrayWritable[2];
		data[0]=arr0;
		data[1]=arr1;
		
		
		LineWritable line=new LineWritable();
		line.set(data);
		
		System.out.println("line="+ line.toString());
		
	}

	/**
	 * needed this method to show data in the output
	 */
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuffer sb=new StringBuffer();
		IntArrayWritable[] arr=(IntArrayWritable[])get();
		for (IntArrayWritable a : arr) {
			sb.append(a.toString()+":");
		}
		return sb.toString();
	}
}