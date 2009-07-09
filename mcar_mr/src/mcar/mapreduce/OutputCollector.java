package mcar.mapreduce;
import java.io.IOException;


public interface OutputCollector<K extends Comparable, V> {
	public void collect(K key, V value) throws IOException;
}
