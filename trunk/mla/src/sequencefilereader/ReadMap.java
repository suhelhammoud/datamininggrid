package sequencefilereader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileAsTextRecordReader;

public class ReadMap {
	final private static String SEP=" ";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//readMap( 5);
		//testStringSplit();

		testKey();
//		for (int i = 0; ; i++) {
//			if (i> 13)break;
//			System.out.println(i);
//		}


	}

	public static void testKey() {
		String[]   s={"1","2","3","4","5","6"};
		String[] tas={"1","2","3","4","5","6","2 3","1 4"};
		HashSet<String> set=new HashSet<String>(Arrays.asList(tas));
//		key(s,2,3,"");
		k(s,0,6,"");
	}

	public static void k(String[] arr,int index,int len,String last){
		if(len<1)return;
			for (int i = index; i < arr.length; i++) {
				//if( ! set.contains(arr[i]))continue;
				System.out.println(last+arr[i]);						
				k(arr, i+1, len-1, last+arr[i]+",");				
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
		try {
			JobConf job=new JobConf();
			FileSystem fs = FileSystem.get(job);
			Path path = new Path("data/"+i+"/part-00000");

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

}
