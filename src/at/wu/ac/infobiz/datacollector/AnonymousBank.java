package at.wu.ac.infobiz.datacollector;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.EndianUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;

import com.google.common.base.Joiner;

/**
 * DataCollector project.
 * 
 * This file is part of a simple toolset to merge / normalize CSV tables.
 * The aim is to preprocess data to later use it in the process mining toolchain (e.g., Disco, ProM).
 * 
 * @author Andreas Rogge-Solti
 *
 */

public class AnonymousBank {

	public static String convertRowsToEvents (File file, Collection<String> activities, Collection<String> attributes) throws IOException{
		Map<String,String> activityColumns = new LinkedHashMap<>();
		for (String act : activities){
			activityColumns.put(act, act);
		}
		Map<String,String> attributeColumns = new LinkedHashMap<>();
		for (String att : attributes){
			attributeColumns.put(att, att);
		}
//		List<String> identifierList = new LinkedList<>();
//		identifierList.add(identifier);
		return convertRowsToEvents(file, activityColumns, attributeColumns);
	}
	
	/**
	 * 
	 * @param file
	 * @param activityColumns contains a mapping of activity columns and their new activity names in the transformed 
	 * @param attributeColumns
	 * @param identifiers
	 * @throws IOException
	 */
	public static String convertRowsToEvents (File inFile, Map<String, String> activityColumns, Map<String,String> attributeColumns) throws IOException{
		String sep = Utils.getDelimiterFromFile(inFile);
		
		List<String> lines = FileUtils.readLines(inFile);
		
		String[] headerParts = lines.get(0).split(sep);
		
		// prepare output
		StringBuilder builder = new StringBuilder();
		
		Iterator<String> lineIter = lines.iterator();
		lineIter.next(); // skip header!
		
		String[] activities = activityColumns.keySet().toArray(new String[activityColumns.size()]);
		Integer[] activityPositions = new Integer[activities.length];
		for (int i = 0; i < activities.length; i++){
			String activity = activities[i];
			activityPositions[i] = ArrayUtils.indexOf(headerParts, activity);
		}
		
		String newHeader = ""; //Joiner.on("//").join(identifiers);
		
		for (String attribute : attributeColumns.keySet()){
			newHeader += attributeColumns.get(attribute);
			newHeader += sep;
		}
		newHeader += "activity";
		newHeader += sep;
		newHeader += "payload";
		newHeader += sep;
		newHeader += "timestamp";
		newHeader += sep;
		newHeader += "id";
		
		builder.append(newHeader).append("\n");
		
		while (lineIter.hasNext()){
			String line = lineIter.next();
			String[] parts = line.split(sep);
			
			Map<String,String> partsMap = new HashMap<>();
			for (int i = 0; i < parts.length; i++){
				partsMap.put(headerParts[i], parts[i]);
			}
			
			// create a record for each activity recorded in the row:
			for (int i = 0; i < activities.length; i++){
				String activity = activities[i];
				String entry = partsMap.get(activity);
				if (entry != null && !entry.isEmpty() && !entry.equals("0:00:00")){
					// create entry:
					List<String> entriesInNewTable = new LinkedList<String>();

//					// id
//					List<String> ids = new LinkedList<>();
//					for (String id : identifiers){
//						ids.add(partsMap.get(id));
//					}
//					String id = Joiner.on("//").join(ids);
//					entriesInNewTable.add(id);
					
					// attributes:
					for (String attribute : attributeColumns.keySet()){
						entriesInNewTable.add(partsMap.get(attribute));
					}
					
					// activity:
					entriesInNewTable.add(activityColumns.get(activity));
					
					// payload (entry):
					entriesInNewTable.add(entry);
					
					// timestamp
					entriesInNewTable.add("19"+partsMap.get("date") + "T"+(entry.length()<8?"0"+entry:entry));

					entriesInNewTable.add(partsMap.get("vru+line")+"_"+partsMap.get("date")+"_"+partsMap.get("call_id"));
					
					// add entry:
					builder.append(Joiner.on(sep).join(entriesInNewTable)).append("\n");
				}
			}
		}
		return builder.toString();
	}
	
	public static void main(String[] args) throws IOException {
		Collection<String> activities = new LinkedList<String>();
		
		Messages.init("anonymousBank");
		
		int i = 0;
		while(getKey(i, KeyType.ACTIVITY) != null){
			String actString = getKey(i, KeyType.ACTIVITY);
			activities.add(actString);
			i++;
		}
		
		i = 0;
		Collection<String> attributes = new LinkedList<>();
		while(getKey(i, KeyType.ATTRIBUTE) != null){
			String key = getKey(i, KeyType.ATTRIBUTE);
			attributes.add(key);
			i++;
		}
		
		for (File inputFile : new File(Messages.getString("input.folder")).listFiles()){
			String result = convertRowsToEvents(inputFile, activities, attributes);
			FileUtils.write(new File(Messages.getString("output.folder")+File.separator+inputFile.getName()), result);	
		}
		
		
//		System.out.println("-----");
//		System.out.println(result);
//		System.out.println("-----");
		
		
	}

	private static String getKey(int activity, KeyType type) {
		if (!Messages.getString(type.toString()+activity).startsWith("!")){
			return Messages.getString(type.toString()+activity);
		}
		return null;
	}
}
