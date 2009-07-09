package dataTypes;

import org.apache.log4j.Logger;

import java.awt.Label;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.*;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;

import dataTypes.fia.Two;
import dm.Sccl;

import others.MyConstants;


//TODO change to HashMap later
public class IntMap extends TreeMap<Integer, List<Integer>> implements 
WritableComparable,MyConstants {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(IntMap.class);

	boolean isRule=false;
	int support,allOcc,minLine, confidence;
	


	public int getConfidene(){
		return confidence;
	}
	public int getSupport() {
		return support;
	}
	public void setSupport(int support) {
		this.support = support;
	}
	public int getAllOcc() {
		return allOcc;
	}
	public void setAllOcc(int allOcc) {
		this.allOcc = allOcc;
	}
	public int getMinLine() {
		return minLine;
	}
	public void setMinLine(int minLine) {
		this.minLine = minLine;
	}
	public IntMap() {
	}
	public void set(Map<Integer, List<Integer>> mp){
		clear();
		putAll(mp);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		clear();
		int sz=in.readInt();
		for (int i = 0; i < sz;i++) {
			int key=in.readInt();
			//			IntListWritable list=new IntListWritable();//IntListWritable.readFieldsFrom(in);
			//			list.readFields(in);
			//List<Integer> list=IntListWritable.readFieldsFrom(in);
			int szList=in.readInt();
			List<Integer> list=new ArrayList<Integer>(szList);
			for (int j = 0; j < szList; j++) {
				list.add(in.readInt());
			}
			put(key, list);
		}
		isRule=in.readBoolean();
		//TODO delete ture later
		if(isRule || true){
			support=in.readInt();
			allOcc=in.readInt();
			minLine=in.readInt();
			confidence=in.readInt();
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size());
		for (Entry<Integer, List<Integer>>  entry : entrySet()) {
			out.writeInt(entry.getKey());
			List<Integer> list=entry.getValue();
			out.writeInt(list.size());
			for (Integer i : list) {
				out.writeInt(i);
			}
		}
		out.writeBoolean(isRule);
		//TODO delete true later
		if(isRule || true){
			out.writeInt(support);
			out.writeInt(allOcc);
			out.writeInt(minLine);
			out.writeInt(confidence);
		}

	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	
	@Override
	public String toString() {		
		StringBuffer sb=new StringBuffer("{");
		
		sb.append("isRule="+isRule+", Occ="+allOcc+", support="+support+", minLine="+minLine +"\n\t");
		for (Entry<Integer,List<Integer>> e : entrySet()) {
			List<Integer> sorted=new ArrayList<Integer>(e.getValue());
			Collections.sort(sorted);
			sb.append(e.getKey()+":"+ sorted+ " , ");
		}
		sb.append("}");
		return sb.toString();
	}

	public void add(int label, int line){
		List<Integer> list=get(label);
		if(list==null){
			list=new ArrayList<Integer>();
			put(label,list);
			list.add(line);
			return;
		}else{
			if(! list.contains(line))
				list.add(line);
			else
				logger.error("contains "+label+", "+line);
		}

	}

	public void addArray(int label, int... arr){
		List<Integer> list=get(label);
		if(list==null){
			list=new ArrayList<Integer>(arr.length);
			put(label,list);
			for(int i: arr)list.add(i);
			return;
		}

		for (int i : arr){
			if(! list.contains(i))
				list.add(i);
			else
				logger.error("contains "+label+", "+i);
		}
	}


	public void addMap(Map<Integer, List<Integer>> map){
		for (Map.Entry<Integer, List<Integer>> e : map.entrySet()) {
			addList(e.getKey(),e.getValue());
		}
	}

	public void addList(int label,List<Integer> list) {
		List<Integer> local=get(label);
		if (local==null)
			put(label, list);
		else{
			for (int i : list){
				if(! local.contains(i))
					local.add(i);
				else
					logger.error("contains "+label+", "+i);
			}
		}
	}
	//TODO to delete later
	<T> boolean containsAny(List<T> list1,List<T> list2){
		HashSet<T> set1=new HashSet<T>(list1);
		return set1.removeAll(list2);
	}

	//max laber,allOccs, lowest line
	public int[] calc(){
		int max=-1;
		int min=Integer.MAX_VALUE;
		int counter=0;
		for (List<Integer> i : values()) {
			int sz=i.size();
			if (sz>max)max=sz;//support
			counter+=sz;//allOcc

			int ln=	Collections.min(i);
			if(ln < min)min=ln;//minLine
		}
		support=max;
		allOcc=counter;
		minLine=min;
		confidence=(int)(Integer.MAX_VALUE*(float) support/(float)allOcc);
		int[] result=new int[4];
		result[TAG.support.ordinal()]=max;
		result[TAG.allOcc.ordinal()]=counter;
		result[TAG.minline.ordinal()]=minLine;
		result[TAG.confidence.ordinal()]=confidence;
		return result;
	}

	public boolean isRule(){
		return isRule;
	}
	public byte[] toBytes_old() throws IOException {
		// Serialize to a byte array
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(bos);
		this.write(out);
		bos.close();
		// Get the bytes of the serialized object
		// byte[] buf = bos.toByteArray();
		return bos.toByteArray();
	}

	public static IntMap fromBytes(byte[] bytes) throws IOException,
	ClassNotFoundException {
		DataInput in = new DataInputStream(new ByteArrayInputStream(bytes));
		IntMap result = new IntMap();
		result.readFields(in);
		return result;
	}


	public void ToLineMapper(OutputCollector<Two, IntListWritable> output,IntListWritable value){
		for (Map.Entry<Integer, List<Integer>> e : this.entrySet()) {

		}
	}

	public static void main(String[] args) {
		IntMap map=new IntMap();
		map.addArray(1,new int[]{1,2,3});
		map.addArray(2,new int[]{221,22,23});

		map.addArray(1, 4,5);
		map.addArray(2,444,4444,4444);

		System.out.println(map);
		map.addArray(1,new int[]{5555551,255555,3});

		System.out.println(map);

	}
	public void setIsRule(boolean b) {
		isRule=b;		
	}


}
