package utils;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

public class TextArray extends ArrayWritable {
	public TextArray() { 
		super(Text.class); 
	}
	
	public static void main(String[] args){

		readMap("data/tree/lined2/part-00000");
		
		
	}
	
	public static void readMap(String filename) {
		try {
			JobConf job=new JobConf();
			FileSystem fs = FileSystem.get(job);
			//Path path = new Path("data/"+i+"/part-00000");
			Path path = new Path(filename);

			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, job);
			try {
				IntWritable key = new IntWritable();
				TextArray value = new TextArray();
				while (reader.next(key, value)) {
					System.out.println(key.toString()+":\t"+value.toString());
				}
			} finally {
				reader.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Text[] get() {
		// TODO Auto-generated method stub
		return (Text[])super.get();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuffer sb=new StringBuffer();
		Text[] arr=get();
		for (Text text : arr) {
			sb.append(text.toString()+",");
		}
		return sb.toString();
	}
	
}