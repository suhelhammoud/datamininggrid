package association.impl1;

import java.awt.ItemSelectable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

import utils.MapTools;
import utils.SetWritable;

public class ItemMap extends TreeMap<SetWritable, Integer>{

	static Logger log=Logger.getLogger(ItemMap.class);
	final private static String SEP=" ";

	public  static int SUPPORT;
	public  static int ITERATION;

	String freqDir;

	private static final IntWritable one=new IntWritable(1);

	JobConf job;

	public void configure(JobConf conf) {
		SUPPORT=conf.getInt("SUPPORT", -1);
		ITERATION=conf.getInt("ITERATION", -1);

		if(SUPPORT == -1 || ITERATION == -1){
			log.error("can't get the support or iteration value");
		}
		this.freqDir =conf.get("freqDir", "data/freqs");
	}


	public static void out(OutputCollector<SetWritable,IntWritable> output,Integer[] arr,int index, int len,SetWritable last)
	throws IOException{
		if (len==1)
			for (int i = index; i < arr.length; i++) {
				last.add(arr[i]);
				if(output==null)
					System.out.println(last);
				else
					output.collect(last, new IntWritable(1));

				last.remove(arr[i]);

			}
		else
			for (int i = index; i < arr.length-len+1; i++) {
				SetWritable newSet=new SetWritable(last);
				newSet.add(arr[i]);
				out(output,arr,i+1,len-1,newSet);
			}
	}


	public static void subsets(Integer[] arr,int index, int len,SetWritable last,List<SetWritable> result){
		if (len==1 ){
			for (int i = index; i < arr.length; i++) {
				SetWritable newSet=new SetWritable(last);
				newSet.add(arr[i]);
				result.add(newSet);
			}
		}
		else
			for (int i = index; i <  arr.length -len+1; i++) {
				SetWritable newSet=new SetWritable(last);
				newSet.add(arr[i]);
				ItemMap.subsets(arr,i+1,len-1,newSet,result);
			} 
	}

	public void getSubsets(Integer[] arr,int index, int len,SetWritable last,List<SetWritable> result){
		if (len==1 ){
			if (  ! containsKey(last)) return;
			for (int i = index; i < arr.length; i++) {
				SetWritable newSet=new SetWritable();
				newSet.add(arr[i]);
				if(! containsKey(newSet))continue;
				newSet.addAll(last);
				result.add(newSet);

			}
		}
		else
			for (int i = index; i <  arr.length -len+1; i++) {
				SetWritable newSet=new SetWritable();
				newSet.add(arr[i]);
				if (  ! containsKey(newSet))continue;
				newSet.addAll(last);
				getSubsets(arr,i+1,len-1,newSet,result);
			} 
	}
	public static List<SetWritable> subsets(Integer[] arr, int index, int len){
		List<SetWritable> result=new ArrayList<SetWritable>();
		ItemMap.subsets(arr, index, len, new SetWritable(), result);
		return result;
	}
	public static List<SetWritable> subsets(SetWritable sw, int index, int len){
		List<SetWritable> result=new ArrayList<SetWritable>();
		Integer[] arr=sw.toArray(new Integer[0]);
		ItemMap.subsets(arr, index, len, new SetWritable(), result);
		return result;
	}

	public List<SetWritable> getSubsets(Integer[] arr, int index, int len){
		List<SetWritable> result=new ArrayList<SetWritable>();
		getSubsets( arr, index, len, new SetWritable(), result);
		return result;
	}
	public List<SetWritable> getSubsets(SetWritable sw, int index, int len){
		List<SetWritable> result=new ArrayList<SetWritable>();
		Integer[] arr=sw.toArray(new Integer[0]);
		getSubsets( arr, index, len, new SetWritable(), result);
		return result;
	}
	public static void main(String[] args)throws Exception{

		Integer[] arr={1,2,3,4,5,6};
		SetWritable sw=new SetWritable(Arrays.asList(arr));
		sw.remove(1);
		Integer[] a2=sw.toArray(new Integer[0]);

		System.out.println(Arrays.toString(a2));
		
		List<Path> pathes=listAllPaths(new Path("data/freqs"));
		
		TreeSet<Path> treepath=new TreeSet<Path>();
		for (int i=pathes.size()-1; i>=0; i--){
			System.out.println(pathes.get(i));
			treepath.add(pathes.get(i));
		}
		System.out.println("paths");
		for (Path path : pathes) {
			System.out.println(path.toString());
			
		}
		System.out.println("treepath");
		for (Path path2 : treepath) {
			System.out.println(path2.toString());
		}
	}


	private static void testSubSets() {
		ItemMap map=new ItemMap();

		//map.put(1, )
		Integer[] arr={0,1,2,3,4,5};
//		out(null, arr, 0, 3, new SetWritable());
		//List<SetWritable> result=subsets(arr, 0, 3);
		System.out.println("-1--------------");

		List<SetWritable> result=ItemMap.subsets(arr, 0, 3);

		for (SetWritable itm : result) {
			System.out.println(itm);
		}

		for (int i = 1; i < arr.length; i++) {
			SetWritable st=new SetWritable();
			st.add(i);
			map.put(st, -1);
		}
		map.put(new SetWritable(), 0);
		List<SetWritable> l2=ItemMap.subsets(arr, 0, 2);
		for (SetWritable i2 : l2) {
			map.put(i2, -2);
		}

		System.out.println("map=="+map+"\n--end map");
		result=map.getSubsets(arr, 0, 3);
		System.out.println("-2---------");

		for (SetWritable stw : result) {
			System.out.println(stw);
		}
	}


	private static void testSetWritableConstructor() {
		Integer[] arr={1,2,3,4,5,6};		
		SetWritable s1=new SetWritable(Arrays.asList(arr));
		System.out.println("s1="+s1);
		SetWritable s2=new SetWritable(s1);
		System.out.println("s2="+s2);
		s2.remove(1);
		System.out.println("after remove");
		System.out.println("s1="+s1);
		System.out.println("s2="+s2);
	}

	public boolean load(JobConf job){
		return load(freqDir+"/"+(ITERATION-1) ,job);
	}


	@Override
	public String toString() {

		StringBuffer sb=new StringBuffer();
		// TODO Auto-generated method stub
		for (SetWritable s : keySet()) {
			sb.append("\n"+s.toString()+":"+get(s));
		}
		return sb.toString();
	}


	public static List<Path> listAllPaths(Path srcPath){
		List<Path> result=new ArrayList<Path>();
		try {
			FileSystem fs=FileSystem.get(new JobConf());
			if( ! fs.exists(srcPath)){
				log.error(" Path"+ srcPath.toString()+" not found");
				return null;
			}
			Path[] paths=fs.listPaths(srcPath);
			for (Path path : paths) {
				if(fs.isFile(path))
					result.add(path);
				else 
					result.addAll(listAllPaths(path));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	public boolean load(String srcDir,JobConf job) {
		//clear();
		configure(job);
		try {
			FileSystem fs = FileSystem.get(job);
			Path srcPath=new Path(srcDir);
			if (! fs.exists(srcPath)) {
				log.error("No map file found");
				return false;
			}
			List<Path> allPathes=listAllPaths(srcPath);
			for (Path path : allPathes) {

				//log.info("load map part + "+path.toString());
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, job);

				SetWritable key = new SetWritable();
				IntWritable value = new IntWritable();
				while (reader.next(key, value)) {

					put(new SetWritable(key), value.get());
					
					//TODO added this bit to map one single items
					for (Integer itm : key) {
						SetWritable aItem=new SetWritable();
						aItem.add(itm);
						if (containsKey(aItem))continue;
						else put(aItem, -1);
					}

				}
				reader.close();
			}
			//TODO check this very important				
			put(new SetWritable(), 0);//needed for IMapper.output()
			return true;

		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}finally{return true;
		}
	}

}
