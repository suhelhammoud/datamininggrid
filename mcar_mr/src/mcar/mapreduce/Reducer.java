package mcar.mapreduce;
import java.io.IOException;
import java.util.Iterator;


public interface Reducer<K2 extends Comparable, V2, K3 extends Comparable, V3> {
	abstract public void reduce(K2 key, Iterator<V2> values,
			OutputCollector<K3, V3> output, Reporter reporter)
	throws IOException;
}
