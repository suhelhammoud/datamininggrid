package hdm;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import dm.Classifier;
import dm.Column;
import dm.Data;
import dm.DataMine;

public class HDataMine extends DataMine {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HDataMine.class);

	HData hData;
	HClassifier hClassifier;
	Map<Long, HColumn> hExistingColumns;
	HBaseConfiguration conf;

	public HDataMine(HBaseConfiguration conf, HData hData) throws IOException {
		this.conf = conf;
		this.hData = hData;
		hExistingColumns = new TreeMap<Long, HColumn>();
		hClassifier = new HClassifier(conf);
	}

	public Map<Long, HColumn> generateAtomicForSupport(int support,
			Set<Integer> availableLines, Data data) {
		HUtil.create(conf, Bytes.toBytes(Long.bitCount(1)));

		Map<Long, HColumn> result = new TreeMap<Long, HColumn>();
		long maxColumnID = maxColumnID();
		for (long i = 1; i <= maxColumnID; i *= 2) {
			// TODO H
			HColumn clmn = null;
			try {
				clmn = new HColumn(i, conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			boolean b = clmn.hGenerateAtomicForSupport(support,
					data.getLines(), data);
			if (b)
				result.put(i, clmn);
		}
		return result;
	}

	public Map<Long, HColumn> hGenerateColumns(double support,
			Set<Integer> availableLines) throws IOException {
		Map<Long, HColumn> result = new TreeMap<Long, HColumn>();
		int oSupport = (int) Math.round(0.49999999999999 + support
				* hData.size());
		logger.info("minimum support =" + oSupport);
		long maxColumnID = maxColumnID();
		for (long i = 1; i <= maxColumnID; i++) {

			if (Long.bitCount(i) == 1) {
				// TODO H
				HColumn clmn = new HColumn(i, null);
				// clmn.generateAtomicOccurances(availableLines, hData);
				clmn.generateAtomicForSupport(oSupport, availableLines, hData);
				if (clmn.size() == 0)
					continue;
				result.put(i, clmn);
			} else {
				long b1 = Column.first(i);
				long b2 = Column.second(i);

				if (!result.containsKey(b1) || !result.containsKey(b2))
					continue;

				HColumn fColumn = result.get(b1);
				HColumn sColumn = result.get(b2);

				// TODO H
				HColumn clmn = new HColumn(i, null);
				clmn.generateForSupport(oSupport, fColumn, sColumn);
				if (clmn.size() == 0)
					continue;
				result.put(i, clmn);
			}
		}
		return result;
	}

	// public Sccl getScclInColumn(String value,long colId){
	// Column clmn=(Column)existingColumns.get(new Long(colId));
	// return clmn.getValueOccurances(value);
	// }

	public void hBuildClassifier(double minRemainInst, double confidence,
			double supp, int numOfItr) throws IOException {
		hClassifier = null;// new HClassifier();

		Set<Integer> workingLines = hData.getLines();
		TreeSet<Integer> uncoveredLines = new TreeSet<Integer>();
		TreeSet<Integer> coveredLines = new TreeSet<Integer>();

		int minOcc = (int) Math.round(hData.size() * supp + 0.5);
		// int minRemainInstOcc = (int) Math.round(size() * minRemainInst +
		// 0.5);

		logger.info("\nMInimum (inside Iterate)" + minOcc);
		int RemainInstOcc = hData.size();

		// Set<Integer> deletedRows = new HashSet<Integer>();
		for (int iter = 1; iter < numOfItr; iter++) {

			// int te = RemainInstOcc;
			hExistingColumns.clear();

			hExistingColumns = hGenerateColumns(supp, workingLines);
			for (HColumn clmn : hExistingColumns.values()) {
				List<Integer> added = clmn.hGenerateForConfidence(hClassifier,
						confidence);
			}
			// check the coverage
			Set<Integer> deletedLines = hClassifier.build(workingLines.size());

			workingLines.removeAll(deletedLines);
			RemainInstOcc -= deletedLines.size();
			hExistingColumns.clear();
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

	public static void main(String[] args) {
		try {
			HBaseConfiguration conf = new HBaseConfiguration();
			HBaseAdmin admin = new HBaseAdmin(conf);

			HData hdata = new HData(conf);
			if (!admin.tableExists("hdata")) {
				hdata.create("hdata");
				hdata.readFromFile("data/in/00.arff", ",");
			}

			Data data = new Data("data/00.arff");

			HDataMine hdataMine = new HDataMine(conf, hdata);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
