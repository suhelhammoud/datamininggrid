package utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.SequenceFileAsTextRecordReader;

public class MapTools {
	final private static String SEP=" ";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//readMap( 5);
		//testStringSplit();

		//testKey();
		//Integer[] lst={0,1,3,5};


		//System.out.println(join("-", "hhh","suhel"));
		//System.out.println(join("-",s,Arrays.asList(lst),-1,null ));
//		for (int i = 0; ; i++) {
//		if (i> 13)break;
//		System.out.println(i);

//		}

		/*
		 * 	public static void testl() {
		String[]   arr={"1","2","3","4","5"};
		String[] tas={ "", "1","3","4","5"};

		//"1 2","1 3","2 3","1 4","2 3","2 4","3 4",	"1 2 3", "1 2 4", "2 3 4", "1 3 4"};
		HashSet<String> set=new HashSet<String>(Arrays.asList(tas));
		ArrayList<Integer> list=new ArrayList<Integer>();
		//list.add(0);
		//list.add(1);

		boolean b=passPrune(set, arr, list, 3);
		System.out.println("test passPrune: "+b);
		try {

			output(null,set,arr,list,0, 2);
			FMap.out(null, set, arr, list, 0, 2);

		} catch (Exception e) {
			// TODO: handle exception
		}
	}

		 */
	}

	public static String join(String FS, String... args) {
		if (args==null) return null;
		if (args.length==0) return "";
		StringBuffer sb = new StringBuffer(args[0]);
		for (int i=1; i<args.length; i++) {
			sb.append(FS+args[i]);
		}
		return sb.toString();
	}	
	public static String join(String FS, Collection set) {
		if (set==null) return null;
		if (set.size()==0) return "";
		Iterator iter=set.iterator();
		StringBuffer sb = new StringBuffer(iter.next().toString());
		while(iter.hasNext()) {
			sb.append(FS+iter.next());
		}
		return sb.toString();
	}	
	public static String join(String FS, String[] arr, List<Integer> list,int except,int arIndex){
		if (arr==null) return null;
		if (arr.length==0) return "";
		if (list.size()==0)return "";


		StringBuffer sb =new StringBuffer();
		for (int i=0; i<list.size(); i++) {
			if(i==except)continue;
			sb.append(arr[list.get(i)]+FS);
		}
		if(except != -1)sb.append(arr[arIndex]);
		return sb.toString();

	}
	public static String join(String FS, String[] arr, List<Integer> list){
		if (arr==null) return null;
		if (arr.length==0) return "";
		if (list.size()==0)return "";

		StringBuffer sb = new StringBuffer(arr[list.get(0)]);
		for (int i=1; i<list.size(); i++) {
			sb.append(FS+arr[list.get(i)]);
		}
		return sb.toString();
	}
	public static String[] joinSplit(String FS, String[] arr, List<Integer> list){
		if (arr==null) return null;
		if (arr.length==0) return null;
		if (list.size()==0)
			return new String[]{"",join(FS,arr)};

		StringBuffer sb1 = new StringBuffer();
		StringBuffer sb2 = new StringBuffer();

		for (int i = 0; i < arr.length; i++) {
			if(list.contains(i))
				sb1.append(FS+arr[i]);
			else
				sb2.append(FS+arr[i]);
		}
		sb1.delete(0, FS.length()-1);
		sb2.delete(0, FS.length()-1);
		return new String[]{sb1.toString(),sb2.toString()};
	}


	public static boolean passPrune(Set<String> set,String[] arr, List<Integer> list, int itm){
		for (int i = 0; i < list.size(); i++) {
			String ts=join(SEP, arr,list,i,itm);
			//System.out.println(ts);
			if(! set.contains(ts))return false;
		}
		return true;
	}
	/**
	 * 
	 * @param output TODO
	 * @param set
	 * @param arr
	 * @param list
	 * @param index
	 * @param len :>=2
	 */
	public static void output(OutputCollector<Text,IntWritable> output,Set<String> set,String[] arr,List<Integer> list,int index, int len)
	throws IOException{
		String last=join(SEP, arr,list);
		if (len==1){
			if( set.contains(last) )
				for (int i = index; i < arr.length; i++) {
					if (passPrune(set, arr, list, i))
						if(output==null)
							System.out.println(last+SEP+arr[i]);
						else
							output.collect(new Text(last+SEP+arr[i]), new IntWritable(1));
				}
		}
		else
			for (int i = index; i < arr.length-len+1; i++) {
				if( ! set.contains(last) ) continue;
				ArrayList<Integer> nlst=new ArrayList<Integer>(list);
				nlst.add(i);
				output(output,set, arr,nlst, i+1, len-1);				
			}
	}
	public static void k(Set<String> set,String[] arr,int index,int len,String last){
		if (len==1){
			if( set.contains(last.trim()))
				for (int i = index; i < arr.length; i++) {
					System.out.println(last+arr[i]);						
				}
		}
		else
			for (int i = index; i < arr.length-len+1; i++) {
				System.out.println(last);
				if( ! set.contains(last.trim() )) continue;
				k(set,arr, i+1, len-1, last+arr[i]+SEP);				
			}
	}
	//all lower combinations if in set
	public static void key(Set<String> set,String[] arr,int index,int len,String last){
		if (len==1)
			for (int i = index; i < arr.length; i++) {
				if( set.contains(arr[i]))
					System.out.println(last+arr[i]);				
			}
		else
			for (int i = index; i < arr.length-len+1; i++) {
				if( ! set.contains(arr[i]))continue;
				key(set,arr, i+1, len-1, last+arr[i]+SEP);				
			}
	}

	//emmit all lower combinations
	public static void key(String[] arr,int index,int len,String last){
		if (len==1)
			for (int i = index; i < arr.length; i++) {
				System.out.println(last+arr[i]);				
			}
		else
			for (int i = index; i < arr.length-len+1; i++) {
				key(arr, i+1, len-1, last+arr[i]+SEP);				
			}
	}

	public static void testStringSplit() {
		//String s=" 		3 4		5    6		";
		String s=" 		 	 		";
		String[] arr=s.split("[ \t\n]");
		System.out.println(arr.length);
		for (String i : arr) {
			if(i.equals(""))continue;
			System.out.println(i);

		}
	}

	public static void readMap(int i) {
		String filename = "data/"+i+"/part-00000";
		readMap(filename);
	}
		public static void readMap(String filename) {
		try {
			JobConf job=new JobConf();
			FileSystem fs = FileSystem.get(job);
			//Path path = new Path("data/"+i+"/part-00000");
			Path path = new Path(filename);

			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, job);
			try {
				Text key = new Text();
				IntWritable value = new IntWritable();
				while (reader.next(key, value)) {
					System.out.println(key.toString()+":\t"+value.get());
				}
			} finally {
				reader.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	public static String SetToString(Set<String> set){
		StringBuffer sb=new StringBuffer("{");
		for (String item : set) {
			sb.append(item+", ");
		}
		sb.append("}");
		return sb.toString();
	}
	

}
