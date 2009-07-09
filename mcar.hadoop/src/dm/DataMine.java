package dm;

import org.apache.log4j.Logger;

import others.Tools;

import java.io.*;
import java.math.BigInteger;
import java.util.*;

enum ColumnType {
	NOT_VALID, ATOMIC, COMPLEX
}

public class DataMine {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(DataMine.class);

	private Data data;
	private Classifier classifier = new Classifier();
	Map<Long, Column> existingColumns;

	/**
	 * 2^(numOfCols-1) -1
	 * 
	 * @param numOfColumns
	 * @return
	 */
	public static long maxColumnID(int numOfColumns) {
		return BigInteger.valueOf(2).pow(numOfColumns - 1).subtract(
				BigInteger.ONE).longValue();
	}

	public Long maxColumnID() {
		return DataMine.maxColumnID(data.getNumberOfColumns());
	}

	public DataMine(Data data) {
		// availableLines= new TreeSet<Integer>(dataMap.keySet());
		this.data = data;
		existingColumns = new TreeMap<Long, Column>();

	}

	public DataMine() {
		// TODO Auto-generated constructor stub
	}

	// public Map<Long,Column> generateAtomicColumnsOccurances(){
	// Map<Long, Column> result=new TreeMap<Long, Column>();
	// long maxColumnID=maxColumnID();
	// for(long i=1; i<=maxColumnID;i*=2){
	// Column clmn=new Column(i);
	// result.put(i,clmn);
	// clmn.generateAtomicOccurances(null, null);
	// }
	// return result;
	// }


	public Map<Long, Column> generateColumns(int oSupport,
			Set<Integer> availableLines) {
Map<Long, Column> result = new TreeMap<Long, Column>();
		
		logger.info("minimum support =" + oSupport);
		long maxColumnID = maxColumnID();
		for (long i = 1; i <= maxColumnID; i++) {

			if (Long.bitCount(i) == 1) {
				Column clmn = new Column(i);
				// clmn.generateAtomicOccurances(availableLines, data);
				clmn.generateAtomicForSupport(oSupport, availableLines, data);
				if (clmn.size() == 0)
					continue;
				result.put(i, clmn);
			} else {
				long b1 = Column.first(i);
				long b2 = Column.second(i);

				if (!result.containsKey(b1) || !result.containsKey(b2))
					continue;

				Column fColumn = result.get(b1);
				Column sColumn = result.get(b2);

				Column clmn = new Column(i);
				clmn.generateForSupport(oSupport, fColumn, sColumn);
				if (clmn.size() == 0)
					continue;
				result.put(i, clmn);
			}
		}
		return result;
	}
	public Map<Long, Column> generateColumns(double support,
			Set<Integer> availableLines) {
		/*
		 * if(Math.abs(tempSupport-support)<0.0000000000001) return false;
		 * tempSupport=support;
		 */
		int oSupport = (int) Math.round(0.49999999999999 + support
				* data.size());
		return generateColumns(oSupport, availableLines);
		
	}

	// public Sccl getScclInColumn(String value,long colId){
	// Column clmn=(Column)existingColumns.get(new Long(colId));
	// return clmn.getValueOccurances(value);
	// }
	

	public void buildClassifier3(double minRemainInst, double conf, double supp,
			int numOfItr) throws IOException {
		classifier = new Classifier();

		Set<Integer> workingLines = data.getLines();
		TreeSet<Integer> uncoveredLines = new TreeSet<Integer>();
		TreeSet<Integer> coveredLines = new TreeSet<Integer>();

		int minOcc = (int) Math.round(data.size() * supp + 0.5);
		// int minRemainInstOcc = (int) Math.round(size() * minRemainInst +
		// 0.5);

		logger.info("\nMInimum (inside Iterate)" + minOcc);
		int RemainInstOcc = data.size();

		// Set<Integer> deletedRows = new HashSet<Integer>();
		for (int iter = 1; iter < numOfItr; iter++) {

			// int te = RemainInstOcc;
			existingColumns.clear();

			existingColumns = generateColumns(supp, workingLines);
			for (Column clmn : existingColumns.values()) {
				classifier.addAllCandidateRules(clmn
						.generateForConfidence(conf));
			}
			// check the coverage
			Set<Integer> deletedLines = classifier.build(workingLines.size());

			workingLines.removeAll(deletedLines);
			RemainInstOcc -= deletedLines.size();
			existingColumns.clear();
		}
		// TODO: add default class
		// addDefaultClass(workingLines);
	}

	public StringBuffer printSupportsAndConfidences(double support,
			double confidence) {
		StringBuffer result = new StringBuffer();
		Map<Long, Column> existingColumns = generateColumns(support, null);
		for (Column clmn : existingColumns.values()) {
			clmn.generateForConfidence(confidence);
			result.append(clmn.toString());
		}
		return result;
	}

	
	// public Set getRuleColValOcc(String cond,int clmn){
	// long clmnNm=1;
	// for(int i=0;i<clmn-1;i++)clmnNm*=2;
	// // JOptionPane.showMessageDialog(null,"clmn"+clmnNm);
	// Column c=(Column)existingColumns.get(new Long(clmnNm));
	// //
	// JOptionPane.showMessageDialog(null,"existing "+existingColumns.size());
	//
	// Integer fstOcc= c.getStringToInteger(cond);
	// if(fstOcc==null)return new HashSet();
	// Sccl sccl=c.getSccl(fstOcc);
	// return sccl.ge;
	// }

	// public void saveWithPrediction(String resultFile) throws IOException {
	// double countSingleClss = 0;
	// double countMultiSched = 0;
	// double countMultiFadi = 0;
	// double countMultiSuhel = 0;
	// double countSingleBest = 0;
	//
	// BufferedWriter out2 = new BufferedWriter(new FileWriter(resultFile));
	// out2.write("add your comments here\n");
	// out2.write("line\t");
	// for (int i = 1; i <= getNumberOfColums(); i++) {
	// out2.write("C" + i + "\t");
	// }
	// out2.write("class\t");
	// out2.write("bClass\t");
	// out2.write("single\t");
	// out2.write("sched\t");
	// out2.write("fadi\t");
	// out2.write("suhel\t");
	// out2.write("bClass\t");
	// out2.write("-->\t");
	// out2.write("Multi Predicted Classes\n");
	//
	// for (Map.Entry<Integer, String[]> e : dataMap.entrySet()) {
	//
	// Integer ti = e.getKey();
	// out2.write(ti.intValue() + "\t");
	// String[] a2 = e.getValue();
	//
	// for (int i = 1; i <= getNumberOfColums(); i++) {
	// out2.write(a2[i] + "\t");
	// }
	// out2.write(a2[0] + "\t"); //write the org. class
	//
	// Rule rl = (Rule) pc.get(ti);
	// //Rule rl=(Rule)mp.get(rlId);
	// String bestClass = rl.getClassLabel(0);
	// int bestOcc = rl.getOccurance(0);
	// int bestJi = 0;
	// for (int ji = 1; ji < rl.getNumberOfClasses(); ji++) {
	// if (rl.getOccurance(ji) > bestOcc) {
	// bestClass = rl.getClassLabel(ji);
	// bestOcc = rl.getOccurance(ji);
	// bestJi = ji;
	// }
	// }
	// boolean b = false;
	// double dd = 0;
	// double singleClss = 0;
	// double muliSched = 0;
	// double multiFadi = 0;
	// double multiSuhel = 0;
	// double singleBest = 0;
	// for (int j = 0; j < rl.getNumberOfClasses(); j++) {
	// if (a2[0].equals(rl.getClassLabel(j))) {
	// if (j == 0) {
	// singleClss = 1.0;
	// countSingleClss += singleClss;
	// }
	// muliSched = 1.0;
	// countMultiSched += muliSched;
	// multiFadi = (double) rl.getOccurance(j) / rl.getOccurance(0);
	// countMultiFadi += multiFadi;
	// multiSuhel = (double) rl.getOccurance(j) / rl.getAllOcc();
	// countMultiSuhel += multiSuhel;
	// if (j == bestJi) {
	// singleBest = 1.0;
	// countSingleBest += singleBest;
	// }
	// break;
	// }
	// ;
	// }
	// out2.write("" + bestClass + "\t");
	// out2.write("" + singleClss + "\t" + muliSched + "\t" + multiFadi + "\t" +
	// multiSuhel + "\t" + singleBest + "\t");
	// out2.write("\t");
	// int space = 12;
	// for (int j = 0; j < rl.getNumberOfClasses(); j++) {
	// out2.write(rl.getClassLabel(j) + "\t");
	// space--;
	// }
	// //added by new suhel add
	// for (; space > 0; space--)
	// out2.write("\t");
	// for (int j = 0; j < rl.getNumberOfClasses(); j++) {
	// out2.write(rl.getOccurance(j) + "\t");
	// }
	// out2.write("\n");
	// }
	// out2.write("singlePred\t");
	// out2.write("" + countSingleClss + "\n");
	// out2.write("multiSched\t");
	// out2.write("" + countMultiSched + "\n");
	// out2.write("multiFadi\t");
	// out2.write("" + countMultiFadi + "\n");
	// out2.write("multiSuhel\t");
	// out2.write("" + countMultiSuhel + "\n");
	// out2.write("singleBest\t");
	// out2.write("" + countSingleBest + "\n");
	// out2.close();
	// }

}
