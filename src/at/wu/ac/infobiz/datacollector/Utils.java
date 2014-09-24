package at.wu.ac.infobiz.datacollector;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

/**
 * DataCollector project.
 * 
 * This file is part of a simple toolset to merge / normalize CSV tables.
 * The aim is to preprocess data to later use it in the process mining toolchain (e.g., Disco, ProM).
 * 
 * @author Andreas Rogge-Solti
 *
 */

public class Utils {

	public static final String[] DELIMITERS = new String[]{";", ",", "\t"};
	
	public static String getDelimiterFromFile(String fileName) throws IOException{
		return getDelimiterFromFile(new File(fileName));
	}
	
	public static String getDelimiterFromFile(File file) throws IOException{
		List<String> lines = FileUtils.readLines(file);
		// header is in first line:
		return getDelimiter(lines.get(0));
	}
	
	/**
	 * Tries to use delimiters as specified in {@link #DELIMITERS}
	 * and find the one that splits the header string into most parts
	 * 
	 * @param header String the line to try to split
	 * @return The delimiter used to successfully split the line.
	 */
	public static String getDelimiter(String header){
		int delimiter = 0;
		
		int maxParts = 0;
		String bestSep = null;
		
		for (int i = 0; i < DELIMITERS.length; i++){
			String sep = DELIMITERS[delimiter];
			String[] parts = header.split(sep);
			if (parts.length > maxParts){
				maxParts = parts.length;
				bestSep = sep;
			}
		}
		return bestSep;
	}
	
	

}
