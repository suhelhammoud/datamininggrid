package others;

public interface MyConstants {
	public final static int SIZEOF_INT=Integer.SIZE/Byte.SIZE;

	public static enum TAG{support,allOcc,minline,confidence};
	public static enum JOB{ support,
							confidence,
							iteration,
							label,
							input_dir,
							items_dir,
							lines_dir,
							labels_dir,
							map_dir,
							SEP,
							inDir,
							outDir
							};
	
	public static enum COUNTERS {
		numbers_of_rows,
		candidate_rules,
		items_left,
		number_of_inputs,
		TIME_FOR_SUBSETS,
		Num_OF_SUBSETS,
		TIME_TO_COLLECT
		} 
}
