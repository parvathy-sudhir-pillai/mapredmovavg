package MovingAverage;

import java.text.FieldPosition;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SmartMeterDataPoint {
	
	private String country;
	private String region = "";
	private String day = "";
	private String offtake = "";
	private String month = "";
	private String year = "";
	private String unit = "";
	private Date entrydate;
	
	public Float getOfftake(){
		return Float.parseFloat(offtake);		
	}
	
	public String getRegion(){
		return region;
	}
	
	public Long getDate(){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		StringBuffer date = new StringBuffer();
		FieldPosition pos = new FieldPosition(0);
		try{
			String d = year+month+day;
			entrydate = sdf.parse(year+month+day);
			return Long.parseLong(sdf.format(entrydate, date , pos).toString());
		}catch(ParseException pe){
			System.out.println(year+"/"+month+"/"+day);
			return Long.parseLong("Error in Date Format");
		}
		}

	@SuppressWarnings("deprecation")
	public static SmartMeterDataPoint parse(String csvRow){
		SmartMeterDataPoint datapoint = new SmartMeterDataPoint();
				
		String values[] = csvRow.split(",");
	    datapoint.country = "New Zealand";
	    for(int i=0 ; i<values.length ; i++)
	      	values[i] = values[i].replaceAll("\"", "");

	    datapoint.region = values[3];
	    datapoint.unit = values[4];
	    datapoint.year = values[0];
	    
	    if(Integer.parseInt(values[1])<=9)
	    	values[1]="0"+values[1];
	    if(Integer.parseInt(values[2])<=9)
	    	values[2]="0"+values[2];

	    datapoint.month = values[1];
	    datapoint.day = values[2];
	    
	    datapoint.offtake = values[5];
	    datapoint.offtake.trim();
		
		return datapoint;
	}
	
}
