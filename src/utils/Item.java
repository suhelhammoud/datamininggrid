package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;

//import tree.Item_old;

public class Item implements WritableComparable {

	static String FS=" ";
	static String IS="-";
	
	static int SUPPORT;
	static float CONFIDENCE;

	TreeSet<Integer > key;
	List<Integer> lines;
	
	public Item(Integer[] arrKey){
		key=new TreeSet<Integer>(Arrays.asList(arrKey));
	}
	
	
	public Item(TreeSet<Integer> stKey){
		this.key=stKey;
	}


	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int keyLength=in.readInt();
		key=new TreeSet<Integer>();
		for (int i = 0; i < keyLength; i++) {
			key.add(in.readInt());
		}
		int linesLength=in.readInt();
		for (int i = 0; i < linesLength; i++) {
			lines.add(in.readInt());
		}
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(key.size());
		for (Integer iter : key) {
			out.writeInt(iter);
		}
		
		if(lines==null){
			out.writeInt(0);
			return;
		}
		out.writeInt(lines.size());
		for (Integer line : lines) {
			out.writeInt(line);
		}
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		Item itm=(Item)o;
		int sz=key.size();
		int diff=sz-itm.key.size();
		if (diff !=0)
			return diff;
		
		Iterator<Integer> iter1=key.iterator();		
		Iterator<Integer> iter2=itm.key.iterator();
		
		for (int i = 0; i < sz;i++) {
			diff=iter1.next()-iter2.next();
			if(diff != 0)
				return diff;
		}
		return 0;
	}

	
	public boolean addLine(Integer line){
		return lines.add(line);
	}
	public void addAllLines(String strLines){
		String[] arrLines=strLines.split(FS);
		for (String ln : arrLines) {
			lines.add(Integer.valueOf(ln));
		}
	}
	public String linesToString(){
		return MapTools.join(FS, lines);
	}
	public String idToString(){
		return MapTools.join(FS, key);
	}
	public int length(){
		return key.size();
	}
	public int numOfLines(){
		return lines.size();
	}

	public boolean isFreq(){
		if(lines.size() > SUPPORT)
			return true;
		else
			return false;
	}
	public IntArrayWritable linesWritable(){
		return new IntArrayWritable(lines);
	}
	public IntArrayWritable keyWritable(){
		return new IntArrayWritable(key);
	}

	public Item merg(Item itm){
		// TODO comment them later
		//if(! isFreq() || ! itm.isFreq())return null;
		//if (length()!=itm.length())return null;
		// TODO check empty items, different size items
		
		TreeSet<Integer> nkey=new TreeSet<Integer>(key);
		nkey.addAll(itm.key);
		if(length() != itm.length()+1 )return null;						
		return new Item(nkey);
	}
	
	public void outputMapper(OutputCollector<IntWritable, Item> output)
	throws IOException{
		for (Integer iter : lines) {
			output.collect(new IntWritable(iter), this);
		}
	}
	
	public void outputReducer(OutputCollector<Item, IntArrayWritable> output)
	throws IOException{
		for (Integer iter : lines) {
			//output.collect(new IntWritable(iter), writableLines());
		}
	}
	
	
	

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return MapTools.join(FS, key);
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
