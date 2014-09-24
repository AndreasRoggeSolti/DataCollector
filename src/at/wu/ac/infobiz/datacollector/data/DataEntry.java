package at.wu.ac.infobiz.datacollector.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class DataEntry<K, V extends Comparable<V>> extends HashMap<K, V> implements Comparable<DataEntry<K,V>>{
	private static final long serialVersionUID = -8841606586394591170L;
	
	private Set<String> keys;
	
	public DataEntry(Set<String> keys){
		super();
		this.keys = keys;
	}

	/**
	 * Compares data entries according to the sorting order of the {@link #keys}.
	 */
	public int compareTo(DataEntry<K, V> other) {
		// compare in order of keys:
		Iterator<String> keyIterator = keys.iterator();
		
		while (keyIterator.hasNext()){
			String key = keyIterator.next();
			if (containsKey(key) && other.containsKey(key)){
				int diff = get(key).compareTo(other.get(key));
				if (diff != 0){
					return diff;
				}
			}
		}
		return 0;
	}

}
