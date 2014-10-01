package at.wu.ac.infobiz.datacollector;


public enum KeyType {
	ATTRIBUTE, ACTIVITY;
	
	public String toString(){
		switch (this) {
		case ATTRIBUTE:
			return "attributes.";
		case ACTIVITY:
		default:
			return "activities.";
		}
	}
}
