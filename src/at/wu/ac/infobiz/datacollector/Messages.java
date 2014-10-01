package at.wu.ac.infobiz.datacollector;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class Messages {
	private static final String BUNDLE_BASE_NAME = "at.wu.ac.infobiz.datacollector."; //$NON-NLS-1$

	private static ResourceBundle resourceBundle; 

	private Messages() {
	}

	public static String getString(String key) {
		try {
			return resourceBundle.getString(key);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		} catch (NullPointerException e){
			System.err.println("Need to call init() first!");
			return null;
		}
	}

	public static void init(String string) {
		resourceBundle = ResourceBundle.getBundle(BUNDLE_BASE_NAME+string);
	}
}
