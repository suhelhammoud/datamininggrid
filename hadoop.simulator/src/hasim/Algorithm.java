package hasim;

public class Algorithm {

	public double taskLength = 1.0;
	public double taskRatio = 0.5;
	public double taskInOutRatio = 1.0;

	public Algorithm(double taskLength, double taskRatio, double taskInOutRatio) {
		super();
		this.taskLength = taskLength;
		this.taskRatio = taskRatio;
		this.taskInOutRatio = taskInOutRatio;
	}

	public static double mapCost = 1.0;
	public static double mapSize = 1.0;
	public static double mapRecords = 1.0;
	
	public static double combineCost = 1.0;
	public static double combineSize = 1.0;
	public static double combineRecords = 1.0;
	
	public static double reduceCost = 1.0;
	public static double reduceSize = 1.0;
	public static double reduceRecords = 1.0;
	

}
