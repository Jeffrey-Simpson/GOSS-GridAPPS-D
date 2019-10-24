package gov.pnnl.goss.gridappsd.data.conversion;

import gov.pnnl.goss.gridappsd.api.DataManager;
import gov.pnnl.goss.gridappsd.api.LogManager;
import gov.pnnl.goss.gridappsd.dto.LogMessage;
import gov.pnnl.goss.gridappsd.dto.LogMessage.LogLevel;
import gov.pnnl.goss.gridappsd.dto.LogMessage.ProcessStatus;
import gov.pnnl.goss.gridappsd.dto.TimeSeriesEntryResult;
import gov.pnnl.goss.gridappsd.dto.TimeSeriesKeyValuePair;
import gov.pnnl.goss.gridappsd.dto.TimeSeriesMeasurementResult;
import gov.pnnl.goss.gridappsd.dto.TimeSeriesResult;
import gov.pnnl.goss.gridappsd.dto.TimeSeriesRowResult;
import gov.pnnl.goss.gridappsd.utils.GridAppsDConstants;

import java.io.InputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.felix.dm.annotation.api.Component;
import org.apache.felix.dm.annotation.api.ServiceDependency;
import org.apache.felix.dm.annotation.api.Start;

@Component
public class ProvenLoadScheduleToGridlabdLoadScheduleConverter implements DataFormatConverter {
	protected static SimpleDateFormat sdfIn = new SimpleDateFormat("MM/dd/yyyy HH:mm");
	protected static SimpleDateFormat sdfOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static String INPUT_FORMAT = "PROVEN_loadprofile";
	public static String OUTPUT_FORMAT = "GRIDLABD_LOAD_SCHEDULE";
	
	
	public static String SOLAR_DIFFUSE = "Diffuse";
	public static String AVG_WIND_SPEED = "AvgWindSpeed";
	public static String AVG_WIND_DIRECTION = "AvgWindDirection";
	public static String HUMIDITY = "TowerRH";
	public static String LONGITUDE = "long";
	public static String LATITUDE = "lat";
	public static String MST = "UTC";
	public static String TEMPERATURE = "TowerDryBulbTemp";
	public static String DATE = "DATE";
	public static String TIME = "time";
	public static String SOLAR_DIRECT = "DirectCH1";
	public static String SOLAR_GLOBAL = "GlobalCM22";
	public static String PLACE = "place";
	
	@ServiceDependency
	private volatile DataManager dataManager;
	@ServiceDependency 
	private volatile LogManager logManager;
	int simulationYear;
	
	
	/*static{
	    sdfIn.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdfOut.setTimeZone(TimeZone.getTimeZone("UTC"));
	}*/
	
	public ProvenLoadScheduleToGridlabdLoadScheduleConverter(){}
	public ProvenLoadScheduleToGridlabdLoadScheduleConverter(LogManager logManager, DataManager dataManager, int simulationYear) {
		this.logManager = logManager;
		this.dataManager = dataManager;
		this.simulationYear = simulationYear;
	}
	
	@Start
	public void start(){
		if(dataManager!=null) {
			dataManager.registerConverter(INPUT_FORMAT, OUTPUT_FORMAT, this);
		}
		else { 
			//TODO send log message and exception
			if(logManager!=null){
				//log.warn("No Data manager available for "+getClass());
				logManager.log(
						new LogMessage(this.getClass().getName(), new Integer(
								0).toString(), new Date().getTime(),
								"No Data manager available for "+getClass(), LogLevel.WARN,
								ProcessStatus.RUNNING, false), "system",
						GridAppsDConstants.topic_platformLog);
			}
		}
	}
	
	
	@Override
	public void convert(String inputContent, PrintWriter outputContent) throws Exception {
		TimeSeriesEntryResult resultObj = TimeSeriesEntryResult.parse(inputContent);
		boolean isFirstRecord = true;
		Calendar c = Calendar.getInstance();
		
		for(HashMap<String,Object> map : resultObj.getData()){
			if(isFirstRecord){
				long longTime = new Double(map.get("time").toString()).longValue(); 
				c.setTime(new Date(longTime*1000));
				c.set(Calendar.YEAR, 2019);
				outputContent.print(sdfOut.format(c.getTime()));
				outputContent.print(" UTC,");
				outputContent.println(map.get("value"));
				isFirstRecord = false;
			}
			else{
				outputContent.print("+1m,");
				outputContent.println(map.get("value"));
			}
		}
	}

	@Override
	public void convert(InputStream inputContent, PrintWriter outputContent)  throws Exception {
		String strContent = IOUtils.toString(inputContent);
		TimeSeriesEntryResult resultObj = TimeSeriesEntryResult.parse(strContent);
		boolean isFirstRecord = true;
		Calendar c = Calendar.getInstance();
		
		for(HashMap<String,Object> map : resultObj.getData()){
			if(isFirstRecord){
				long longTime = new Double(map.get("time").toString()).longValue(); 
				c.setTime(new Date(longTime*1000));
				c.set(Calendar.YEAR, 2013);
				outputContent.print(sdfOut.format(c.getTime())+",");
				outputContent.println(map.get("value"));
				isFirstRecord = false;
			}
			else{
				outputContent.print("+1m,");
				outputContent.println(map.get("value"));
				isFirstRecord = false;
			}
		}
	}
}
