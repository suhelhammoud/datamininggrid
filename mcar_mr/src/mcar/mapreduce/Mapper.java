package mcar.mapreduce;
import java.io.IOException;


public interface Mapper<K1 extends Comparable , V1, K2 extends Comparable, V2> {
	public void map(K1 key, V1 value, OutputCollector<K2, V2> output,
			Reporter reporter) throws IOException ;
}
