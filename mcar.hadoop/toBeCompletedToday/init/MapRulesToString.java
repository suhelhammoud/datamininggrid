package init;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

import others.Tools;
import dataTypes.IntListWritable;
import dataTypes.IntMap;

public class MapRulesToString {
	List<List<Integer>> labels=new ArrayList<List<Integer>>();
	List<List<Integer>> condistions=new ArrayList<List<Integer>>();
	List<List<Integer>> scondistions=new ArrayList<List<Integer>>();
	
	Map<Integer, List<Integer>> tmap=new TreeMap<Integer, List<Integer>>();



	public void read(String mapDir) {
		JobConf job=new JobConf();
		try {
			FileSystem fs = FileSystem.get(job);
			Path srcPath=new Path(mapDir);
			if (! fs.exists(srcPath)) {
				System.out.println("No map file found");
				return ;
			}
			List<Path> paths=Tools.listAllFiles(srcPath);
			for (Path path : paths) {
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, job);

				IntListWritable key = new IntListWritable();
				IntListWritable value=new IntListWritable();

				while (reader.next(key, value)) {

					int line=key.get(key.size()-1);
					List<Integer> list=tmap.get(line);
					if(list==null){
						list=new ArrayList<Integer>();
					}
					list.add(labels.size());
					tmap.put(line, list);

					value.add(key.get(1));
					key.remove(0);key.remove(0);key.remove(key.size()-1);

					labels.add(new ArrayList<Integer>(value));
					condistions.add(new ArrayList<Integer>(key));
				}
				reader.close();

			}			

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static List<Integer> getMapped(List<Integer> line,List<Integer> cols){
		List<Integer> result=new ArrayList<Integer>(line.size()-1);
		for (int i = 0; i < line.size()-1; i++)result.add(0);
		Collections.fill(result, 0);
		for (int i : cols) {
			result.set(i, line.get(i));
		}
		return result;
	}
	
	public void fill(String dataFile){
		for (int i = 0; i < condistions.size(); i++) {
			scondistions.add(null);
		}
		JobConf job=new JobConf();
		try {
			FileSystem fs = FileSystem.get(job);
			Path srcPath=new Path(dataFile);
			if (! fs.exists(srcPath)) {
				System.out.println("No data file found");
				return ;
			}
			List<Path> paths=Tools.listAllFiles(srcPath);
			for (Path path : paths) {
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, job);

				IntWritable key = new IntWritable();
				IntListWritable value=new IntListWritable();

				Iterator<Integer> iter=tmap.keySet().iterator();
				int current=-1;

				if(iter.hasNext()) current=iter.next();
				else return;


				while(reader.next(key, value)){
					int line=key.get();
					if(line==current){
						System.out.println(key+"\t"+value);
						for (Integer i : tmap.get(current)) {
							scondistions.set(i, getMapped(value, condistions.get(i)));
							System.out.println("\t\t"+condistions.get(i));
						}
						if(iter.hasNext())current=iter.next();
						else return;
					}


				}
				reader.close();

			}			

		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	@Override
	public String toString() {
		StringBuffer sb=new StringBuffer();
		for (int i = 0; i < condistions.size(); i++) {
			sb.append("\n"+scondistions.get(i)+"\t"+condistions.get(i)+"\t"+labels.get(i));
			
		}
		return sb.toString();
	}

	public void reserve(){
		Collections.reverse(condistions);
		Collections.reverse(scondistions);
		Collections.reverse(labels);
	}

	public static void main(String[] args) {
		MapRulesToString mrs=new MapRulesToString();
		mrs.read("data/rules/items");
		System.out.println(Tools.join(mrs.tmap.entrySet(),"\n"));
		System.out.println(Tools.join(mrs.condistions,"\n"));

		System.out.println("------------------");
		

		mrs.fill("data/in/input");
		mrs.reserve();
		System.out.println("0d0d0d0d0d0d\n"+mrs);
		System.out.println("size "+ mrs.scondistions.size());
		
//		System.out.println(mrs.labels);
	}

}
