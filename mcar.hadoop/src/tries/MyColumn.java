package tries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import dataTypes.IntListWritable;
import others.Tools;

import dataTypes.IntListWritable;
import dataTypes.IntMap;
import dataTypes.fia.IntTextPair;
import dm.Column;
import dm.Data;
import dm.DataMine;
import dm.Sccl;


public class MyColumn extends TreeMap<List<Integer>,Sccl> {

	
	public MyColumn(String fileName) {
		read(fileName);
	}
	
	public MyColumn() {
	}
	
	public static MyColumn getMyColumn(String fileName,int support){
		//double confidance=0.4;

		Data data=new Data("data/in/arff/00.arff");
		DataMine datamine=new DataMine(data);

		MyColumn m1=new MyColumn();
		Map<Long,Column> result=datamine.generateColumns(support, data.getLines());
		for (Column  clmn : result.values()) {
			int clmnId=(int)clmn.columnId;
			for (Map.Entry<Integer, Sccl> iter : clmn.getItems().entrySet()) {
				List<Integer> lst=new IntListWritable();
				lst.add(clmnId);lst.add(iter.getKey());
				m1.put(lst, iter.getValue());
			}
		}
		
		return m1;
	}
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
				IntMap value=new IntMap();
				while (reader.next(key, value)) {
					IntListWritable nk=new IntListWritable();
					nk.add((int)getid(key));
					nk.add(key.get(key.size()-1));
					put(nk,IntMapToSccl(value, key));
				}
				reader.close();
				
			}			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static long getid(List<Integer> list){
		long result=0;
		for (int i = 0; i < list.size()-1; i++) {
			result+=Math.pow(2, list.get(i));
		}
		return result;
	}
	public static Sccl IntMapToSccl(IntMap map, List<Integer> id){
		Sccl sccl=new Sccl(getid(id));
		for (Map.Entry<Integer, List<Integer>> iter : map.entrySet()) {
			for (Integer i : iter.getValue()) {
				sccl.addLine(i, iter.getKey());
			}
		}
		sccl.getSupport();
		sccl.getConfidence();
		sccl.minimumLine();
		sccl.getRowId();
		return sccl;
	}
	
	@Override
	public String toString() {
		StringBuffer sb=new StringBuffer();
		for (Map.Entry<List<Integer>, Sccl > iter : this.entrySet()) {
			sb.append(full(iter.getKey())+ "\t"+ iter.getValue()+"\n");
		}
		
		return sb.toString();
	}
	
	public static List<Integer> full(List<Integer> list){
		int[] arr=Data.bitsLocations(list.get(0));
		List<Integer> result=new ArrayList<Integer>();
		for (int i : arr) {
			result.add(i);
		}
		result.add(list.get(1));
		return result;
		
	}
	

	
}
