package at.wu.ac.infobiz.datacollector;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;

import com.google.common.base.Joiner;

import at.wu.ac.infobiz.datacollector.data.DataEntry;

/**
 * DataCollector project.
 * 
 * This file is part of a simple toolset to merge / normalize CSV tables.
 * The aim is to preprocess data to later use it in the process mining toolchain (e.g., Disco, ProM).
 * 
 * @author Andreas Rogge-Solti
 *
 */
public class Main {
	
	public static final String SOURCE_STRING = "sourceFile";
	
	public static Map<String,String> keyMappings = new HashMap<>();
	static {
		keyMappings.put("tag_date", "0_Datum");
		keyMappings.put("Datum", "0_Datum");
		keyMappings.put("epc", "0_Epc");
	}
	
	public static final String keyId = "0_Epc"; 
	
	public static Map<String,Map<String,String>> valueMappings = new HashMap<>();
	static {
		
//		201	WE Motex
//		425	Umlagerung zwischen 01 und 02
//		426	Bestandsentstehung
//		526	Bestandsaufnhame
//		317	Etikettendruck durch HH
//		318	Etikettendruck aus WWS
//		701	Etikettenanforderung
		Map<String, String> mappings = new HashMap<>();
		mappings.put("201", "WE Motex");
		mappings.put("425", "Umlagerung zwischen 01 und 02");
		mappings.put("426", "Bestandsentstehung");
		mappings.put("526", "Bestandsaufnahme");
		mappings.put("317", "Etikettendruck durch HH");
		mappings.put("318", "Etikettendruck aus WWS");
		mappings.put("701", "Etikettenanforderung");
		valueMappings.put("bu_art", mappings);
	}
	
	public static void open(File[] files) throws IOException{
		

		
		
		Set<String> keys = new TreeSet<String>();
		keys.add(SOURCE_STRING);
		
		Set<DataEntry<String, String>> entries = new TreeSet<>();
		Map<String, Map<String,Integer>> occurrences = new HashMap<>();
		
		List<String> fileNames = new LinkedList<>();
		String sep = null;
		for (File file : files){
			List<String> lines = FileUtils.readLines(file);
			String fileName = file.getName();
			fileNames.add(fileName);
			
			// header is in first line:
			String header = lines.get(0);
			// try delimiters:
			sep = Utils.getDelimiter(header);
			String[] parts = header.split(sep);
			for (int k = 0; k < parts.length; k++){
				if (keyMappings.containsKey(parts[k])){
					parts[k] = keyMappings.get(parts[k]);
				}
			}
			keys.addAll(Arrays.asList(parts));
			System.out.println("Using delimiter "+ sep);
			System.out.println("Current keys: "+ ArrayUtils.toString(keys.toArray()));
			
			for (int i = 1; i < lines.size(); i++){
				// read lines:
				String[] lineParts = lines.get(i).split(sep);
				DataEntry<String, String> entry = new DataEntry<>(keys);
				for (int p = 0; p < parts.length; p++){
					if (valueMappings.containsKey(parts[p])){
						if (valueMappings.get(parts[p]).containsKey(lineParts[p])){
							lineParts[p] = valueMappings.get(parts[p]).get(lineParts[p]);
						}
					}
					if (parts[p].equals(keyId)){
						if (!occurrences.containsKey(lineParts[p])){
							occurrences.put(lineParts[p], new HashMap<String, Integer>());
						}
						if (!occurrences.get(lineParts[p]).containsKey(fileName)){
							occurrences.get(lineParts[p]).put(fileName, 0);
						}
						occurrences.get(lineParts[p]).put(fileName, occurrences.get(lineParts[p]).get(fileName) + 1 );
					}
					entry.put(parts[p], lineParts[p]);
				}
				entry.put(SOURCE_STRING, fileName);
				entries.add(entry);
			}
		}
		
		// Todo output everything again:
		File outfile = new File("out/merge"+System.currentTimeMillis()+".csv");
		outfile.createNewFile();
		
		StringBuilder output = new StringBuilder();
		output.append(Joiner.on(sep).join(keys)).append("\n");
		for (DataEntry<String, String> entry : entries){
			Iterator<String> keyIter = keys.iterator();
			while (keyIter.hasNext()){
				String key = keyIter.next();
				
				String val = entry.containsKey(key) ? entry.get(key).toString() : "";
				output.append(val);
				if (keyIter.hasNext()){
					output.append(sep);
				}
			}
			output.append("\n");
		}
		FileUtils.write(outfile, output);
		
		
		for (String fileName : fileNames){
			System.out.print(fileName);
			System.out.print(sep);
		}
		System.out.println();
		
		// print EPCs, which appear in buchungen and in either of the other files!
		List<String> sharedEpcs = new LinkedList<>();
		List<String> epcsWithMoreThanOneAvis = new LinkedList<>();
		
		for (String key : occurrences.keySet()){
			if (occurrences.get(key).containsKey(fileNames.get(1))){
				if (occurrences.get(key).containsKey(fileNames.get(0)) || occurrences.get(key).containsKey(fileNames.get(2))){
					sharedEpcs.add(key);
				}
			}
			if (occurrences.get(key).containsKey(fileNames.get(0)) && occurrences.get(key).get(fileNames.get(0)) > 1){
				epcsWithMoreThanOneAvis.add(key);
			}
			for (String fileName : fileNames){
				Integer occurred = occurrences.get(key).containsKey(fileName)?occurrences.get(key).get(fileName):0;
				System.out.print(occurred + sep);
			}
			System.out.println();
		}
		// shared EPCs:
		System.out.println("\nShared EPCs:");
		System.out.println(ArrayUtils.toString(sharedEpcs.toArray()));
	
		// EPCs with more than one avis
		System.out.println("\nEPCs with more than one entry in Avis:");
		System.out.println(ArrayUtils.toString(epcsWithMoreThanOneAvis.toArray()));
		
	}
	
	
	public static void main(String[] args) throws IOException {
		// assume that args contain files to merge into a combined information store
		
		// for now let's focus on csv files:
		File[] files = new File("data").listFiles();
		open(files);
	}
}
