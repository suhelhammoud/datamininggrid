package others;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.text.*;
import java.io.*;
import java.util.*;

/**
 * <p>
 * Title: data mining
 * </p>
 * <p>
 * Description: fadi project
 * </p>
 * <p>
 * Copyright: Copyright (c) 2003
 * </p>
 * <p>
 * Company: Bradford University
 * </p>
 * 
 * @version 1.0
 */

public class Tools {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Tools.class);

	static DecimalFormat tt = new DecimalFormat("000000.000000");

	public Tools() {
	}

	public static String f(double d) {
		return tt.format(d);
	}

	public static <T> StringBuffer join(T[] c, String delema) {
		StringBuffer result = new StringBuffer();
		if (c == null) {
			logger.error("null array");
			return result;
		}
		if (c.length > 0)
			result.append(c[0].toString());
		else
			return result;
		for (int i = 1; i < c.length; i++) {
			result.append(delema + c[i]);
		}
		return result;
	}

	public static <T> String join(Collection<T> c, String delema) {
		StringBuffer result = new StringBuffer();
		if (c == null) {
			logger.error("null collection");
			return result.toString();
		}
		Iterator<T> iter = c.iterator();
		if (iter.hasNext())
			result.append(iter.next().toString());
		else
			return result.toString();

		while (iter.hasNext()) {
			result.append(delema + iter.next().toString());
		}
		return result.toString();
	}

	public static StringBuffer join(int[] c, String sep) {
		StringBuffer result = new StringBuffer("");
		if (c == null || c.length == 0)
			return result;
		result.append(c[0]);
		for (int i = 1; i < c.length; i++) {
			result.append(sep + c[i]);
		}
		return result;

	}

	public static void main(String[] args) throws IOException {
		List<Path> list=listAllDirs(new Path("data/in"));
		System.out.println(join(list,"\n"));
		System.out.println(list.size());

	}

	public static void shuffleFile(String PInputFileName, String outFileName) {
		List<String> result = new ArrayList<String>();
		try {
			BufferedReader in = new BufferedReader(new FileReader(
					PInputFileName));
			String row;
			while ((row = in.readLine()) != null) {
				result.add(row);
			}
			in.close();

			Collections.shuffle(result);
			BufferedWriter out = new BufferedWriter(new FileWriter(outFileName));
			for (String line : result) {
				out.write(line + "\n");
			}
			out.flush();
			out.close();

		} catch (IOException ioe) {
		}
		Collections.shuffle(result);

	}

	public static String join(long[] a, String d) {
		StringBuffer result = new StringBuffer("" + a[0]);
		for (int i = 1; i < a.length; i++)
			result.append(d + a[i]);
		return result.toString();
	}

	// public static int[] getRandomSamp(int requiredLines,int size){
	// int[] result=new int[requiredLines];
	// Set ts=new HashSet();
	// int count=0;
	// while(count<requiredLines){
	// int num=(int)Math.round(Math.random()*size-0.5);
	// if(!ts.contains(new Integer(num))){
	// ts.add(new Integer(num));
	// result[count]=num;
	// count++;
	// }
	// }
	// return result;
	// }
	/*
	 * b[0]=conf b[1]=supp b[2]=1- len/(cols num) //column Length
	 * b[3]=occurances/total b[3]=1- colId/total b[4]=1- rowId/total
	 * 
	 * a[0]=conf shift a[1]=supp sift a[2]=len shift a[3]=occo shift a[4]=colId
	 * shift
	 */
	public static long setItemId(long columnName, int itemLine) {
		long result = columnName * 1000000 + itemLine;
		return result;
	}

	static double ruleOrder(int[] a, double[] b) {
		// for(int k=0;k<b.length; k++){
		// System.out.print(b[k]+"\t");
		// }
		double sum = 0.0;
		for (int i = 0; i < 5; i++) {
			sum = Math.round((sum + Math.abs(b[i])) * Math.pow(10, a[i]));
			// System.out.println(tt.format(sum));
		}
		sum += b[5];
		// System.out.println(tt.format(sum));
		// DecimalFormat tt=new
		// DecimalFormat("0000000000000000000000000000.000000000");

		return sum;
	}

	static Set<int[]> decart(Set<Integer> c1, Set<Integer> c2) {
		Set<int[]> ss = new HashSet<int[]>(c1.size() * c2.size());
		Set<Integer> cc1 = new HashSet<Integer>(c1);
		if (cc1.size() == 0)
			System.out.println("S1 is empty");
		Set<Integer> cc2 = new HashSet<Integer>(c2);
		if (cc2.size() == 0)
			System.out.println("S2 is empty");
		int i = 1;
		Iterator<Integer> i1 = cc1.iterator();
		while (i1.hasNext()) {
			Iterator<Integer> i2 = cc2.iterator();
			int tntn = i1.next();
			while (i2.hasNext()) {
				int[] a = new int[2];
				a[0] = tntn;
				a[1] = i2.next();
				ss.add(a);
			}
		}
		return new HashSet<int[]>(ss);
	}

	public static void saveToFile(Collection testSet, Map testMap, String fs)
	throws IOException {
		BufferedWriter out2 = new BufferedWriter(new FileWriter(fs + ".xls"));
		Iterator iter = testSet.iterator();
		while (iter.hasNext()) {
			Object o = (Object) testMap.get(iter.next());
			out2.write(o + "\n");
		}
		out2.close();
	}

	/**
	 * 
	 * @param srcPath
	 * @return list of all files in directories under the srcPath directory
	 */
	public static List<Path> listAllFiles(Path path){
		List<Path> result=new ArrayList<Path>();
		try {
			FileSystem fs=FileSystem.get(new JobConf());
			if( ! fs.exists(path)){
				logger.error(" Path"+ path+" not found");
				return null;
			}
			FileStatus[] arrStatus=fs.listStatus(path);
			for (FileStatus i : arrStatus) {
				if( ! i.isDir())
					result.add(i.getPath());
				else 
					result.addAll(listAllFiles(i.getPath()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public static List<Path> listAllDirs(Path path){
		List<Path> result=new ArrayList<Path>();
		try {
			FileSystem fs=FileSystem.get(new JobConf());
			if( ! fs.exists(path)){
				logger.error(" Path"+ path+" not found");
				return null;
			}
			FileStatus[] arrStatus=fs.listStatus(path);
			for (FileStatus i : arrStatus) {
				if( i.isDir()){
					result.add(i.getPath());
					result.addAll(listAllDirs(i.getPath()));

				} 
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	public static void addLines(String inDir,String outDir) throws IOException{
		logger.info("("+inDir+","+outDir+")");
		JobConf job=new JobConf();
		FileSystem fs = FileSystem.get(job);
		Path inPath=new Path(inDir);
		Path outPath=new Path(outDir);
		if( fs.exists(outPath))fs.delete(outPath, true);
		fs.mkdirs(outPath);


		List<Path> files=listAllFiles(inPath);
		System.out.println(join(files, "\n"));
		System.out.println();
		if(files.size()==0)return;
		int line=1;
		for (Path file : files) {
			FSDataOutputStream out = fs.create(new Path(outDir+"/"+line));
			FSDataInputStream in = fs.open(file);

			BufferedReader b=new BufferedReader(new InputStreamReader(in));
			BufferedWriter w=new BufferedWriter(new OutputStreamWriter(out));
			String dataLine;
			while((dataLine=b.readLine()) != null ){
				w.write(String.valueOf(line++)+","+dataLine);
				w.newLine();
			}
			w.flush();
			b.close();
			w.close();
		}
	}

}

/*
 * class timeMeasure{
 * 
 * Date d1,d2,tempD; long t1,t2,tempT=0,sumT=0;
 * 
 * public timeMeasure(){ d1=new Date(); } public timeMeasure(Date d){ d1=d; }
 * public timeMeasure(long d){ d1= new Date(d); }
 * 
 * public void setStartDate(Date d){ d1=new Date(d.getTime()); tempT=0; }
 * 
 * public void setStartDate(){ d1=new Date(); tempT=0; }
 * 
 * public start(){ d1=new Date(); tempT=0; sumT=0; }
 * 
 * public void stop(){ d2=new Date(); sumT=d2.getTime()-tempT.getTime(); }
 * 
 * public void pause(){ d2=new date(); sumT+=d2.getTime()-tempT.getTime(); }
 * 
 * public void play(){ tempT=new date(); }
 * 
 * public void setTime(long t){ d1=new Date(t); }
 * 
 * public long getCurrentTime(){ return new Date().getTime()-d1.getTime; }
 * 
 * public date getStartTime(){ return d1.getTime(); } public Date
 * getStartDate(){ return d1; } }
 */

class TicToc {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(TicToc.class);

	Date start;
	String msg;

	public TicToc(String msg, Date d) {
		this.msg = msg;
		this.start = new Date(d.getTime());
	}

	public TicToc() {
		this("", new Date());
	}

	public TicToc(Date d) {
		this("", d);
	}

	public long tic(Date d) {
		start = new Date(d.getTime());
		return start.getTime();
	}

	public long tic(TicToc tt) {

		return tic(tt.start);
	}

	public long tic() {
		return tic(new Date());
	}

	public long toc() {
		return new Date().getTime() - start.getTime();
	}

}
