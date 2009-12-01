package dfs;

import gridsim.Gridlet;

public class DatumGridlet extends Gridlet {

	int numOfEntries = 1000;
	double keyValueTotalRate = 0.25;
	int destResourceId;
	int splitId;

	public DatumGridlet(int splitId, int gridletID, long gridletLength,
			int gridletFileSize, int gridletOutputSize, int record,
			int numOfEntries, double keyValueTotalRate) {
		super(gridletID, gridletLength, gridletFileSize, gridletOutputSize,
				record);

		this.splitId = splitId;
		this.numOfEntries = numOfEntries;
		this.keyValueTotalRate = keyValueTotalRate;
	}

	public int getDestResourceId() {
		return destResourceId;
	}

	public void setDestResourceId(int destResourceId) {
		this.destResourceId = destResourceId;
	}

}
