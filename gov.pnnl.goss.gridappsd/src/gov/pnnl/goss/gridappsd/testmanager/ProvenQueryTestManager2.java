package gov.pnnl.goss.gridappsd.testmanager;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gov.pnnl.goss.gridappsd.data.ProvenTimeSeriesDataManagerImpl;
import gov.pnnl.goss.gridappsd.dto.RequestTimeseriesData;
import gov.pnnl.proven.api.exception.NullExchangeInfoException;
import gov.pnnl.proven.api.exception.SendMessageException;
import gov.pnnl.proven.api.producer.ProvenProducer;
import gov.pnnl.proven.api.producer.ProvenResponse;
import gov.pnnl.proven.message.exception.InvalidProvenMessageException;

public class ProvenQueryTestManager2 {
	String provenUri = "http://localhost:18080/hybrid/rest/v1/repository/provenMessage";
	ProvenTimeSeriesDataManagerImpl provenTimeSeriesDataManager = new ProvenTimeSeriesDataManagerImpl();
	
	
	public String getListOfTime(String  simulationId, String expected_output_series){
		CompareResults compareResults = new CompareResults();
		Map<String, JsonElement> expectedOutputMap = compareResults.getExpectedOutputMap("0", expected_output_series);
		Set<String> keySet = expectedOutputMap.keySet();
//		String first = keySet.iterator().next();
				
//		return query(simulationId,"_e10b535c-79f3-498b-a38f-11d1cc50f3a0", null,null,null).result.toString();
//		return timeSeriesQuery(simulationId,"_e10b535c-79f3-498b-a38f-11d1cc50f3a0", null,null);
		for (String mrid : keySet) {
//			String response = query(simulationId, mrid, null,null,null).result.toString();
			String response = timeSeriesQuery(simulationId, mrid, null,null);
			if (response.contains("PROVEN_MEASUREMENT"))
				return response;
		}
		return null;
	}

	public TestResultSeries test_proven(String simulationId, String expected_output_series){
	
//		String expected_output_series = "/home/gridappsd/gridappsd_project/sources/GOSS-GridAPPS-D/gov.pnnl.goss.gridappsd/test/gov/pnnl/goss/gridappsd/expected_output_series3.json";
//		String expected_output_series = "/Users/jsimpson/git/adms/gridappsd-sample-app/sample_app/tests/expected_result_series_123_full.json";
//		String simulationId = "1278337149";
//		ProvenTimeSeriesDataManagerImpl pm = new ProvenTimeSeriesDataManagerImpl();
//		RequestTimeseriesData rs = new  RequestTimeseriesData();
//		List<String> keywords = null;

//		provenUri = "http://proven:8080/hybrid/rest/v1/repository/provenMessage";

//		mySend(provenUri);
		
		String responseStr = null;
//		responseStr = query(simulationId, null, "1532971828475", null, keywords).result.toString();
		
//		System.out.println(response.result.toString());
//		processWithAllTimes(expected_output_series, simulationId, response);
		
		
		String responses = getListOfTime(simulationId, expected_output_series);
		Set<String> times = getTimes(responses);
		TestResultSeries testResultSeries = new TestResultSeries();
		int index = 0;
		for (String time : times) {

			responseStr = timeSeriesQuery(simulationId, null, time, time);
			
//			responseStr = query(simulationId, null, time, time,  keywords).result.toString();
			processATime(expected_output_series, simulationId, responseStr, testResultSeries, index+"");
			index++;
		}
		
		System.out.println("TestManager number of conflicts: "+ " total " + testResultSeries.getTotal());
		return testResultSeries;
	}

	private String timeSeriesQuery(String simulationId, String hasMrid, String startTime, String endTime) {
		RequestTimeseriesData request = new RequestTimeseriesData();
		request.setSimulationId(simulationId);
		request.setMrid(hasMrid);
		request.setStartTime(startTime);
		request.setEndTime(endTime);
		String responseStr = null;
		try {
			responseStr = provenTimeSeriesDataManager.query(request);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return responseStr;
	}
	
	private void processATime(String expected_output_series, String simulationId, String responseStr, TestResultSeries testResultSeries, String index) {
		CompareResults compareResults = new CompareResults();
		JsonObject jsonObject = CompareResults.getSimulationJson(responseStr);
		JsonObject simOutputObject = new JsonObject();
		JsonArray meas_array = new JsonArray();
		String time = null;
		JsonArray measurements = new JsonArray();
		
		JsonArray ma = jsonObject.get("measurements").getAsJsonArray();
		for (JsonElement meas : ma) {
			JsonArray points_array = meas.getAsJsonObject().get("points").getAsJsonArray();
			for (JsonElement point : points_array) {
				JsonArray entry_array = point.getAsJsonObject().get("row").getAsJsonObject().get("entry").getAsJsonArray();
//				System.out.println(" ");
				JsonObject innerObject = new JsonObject();
				meas_array.add(innerObject);
				for (JsonElement entry : entry_array) {
					buildMeasurementObject(innerObject, entry);
				}
				time = innerObject.get("timestamp").getAsString();
				innerObject.remove("timestamp");
				measurements.add(innerObject);
			}
		}

		simOutputObject.addProperty("timestame", time);
		simOutputObject.add("measurements", measurements);
		JsonObject msgObject = new JsonObject();
		msgObject.addProperty("simulation_id", simulationId);
		msgObject.add("message", simOutputObject);

		JsonObject outputObject = new JsonObject();
		outputObject.add("output", msgObject);
		System.out.println(simOutputObject.toString());
		TestResults tr = compareResults.compareExpectedWithSimulationOutput(index+"", outputObject, expected_output_series);
		if (tr != null) {
			testResultSeries.add(index+"", tr);
		}
	}

	public void buildMeasurementObject(JsonObject innerObject, JsonElement entry) {
		String key = entry.getAsJsonObject().get("key").getAsString();
		if(key.equals("hasValue")){
			float value = entry.getAsJsonObject().get("value").getAsFloat();
			innerObject.addProperty("value", value);
		}
		if(key.equals("hasAngle")){
			float value = entry.getAsJsonObject().get("value").getAsFloat();
			innerObject.addProperty("angle", value);
		}
		if(key.equals("hasMagnitude")){
			float value = entry.getAsJsonObject().get("value").getAsFloat();
			innerObject.addProperty("magnitude", value);
		}
		if(key.equals("hasMrid")){
			String value = entry.getAsJsonObject().get("value").getAsString();
			innerObject.addProperty("measurement_mrid", value);
		}
		if(key.equals("time")){
			String value = entry.getAsJsonObject().get("value").getAsString();
			innerObject.addProperty("timestamp", value);
		}
	}

	public void processWithAllTimes(String expected_output_series, String simulationId, ProvenResponse response) {
		TestResultSeries testResultSeries = new TestResultSeries();
		CompareResults compareResults = new CompareResults();
		JsonObject jsonObject = CompareResults.getSimulationJson(response.data.toString());
//		JsonObject jsonObject = CompareResults.getSimulationJson(message.toString());
		JsonObject simOutputObject = new JsonObject();
		JsonArray meas_array = new JsonArray();
		JsonObject temp_times = new JsonObject();

		
		JsonArray ma = jsonObject.get("measurements").getAsJsonArray();
		for (JsonElement meas : ma) {
			JsonArray points_array = meas.getAsJsonObject().get("points").getAsJsonArray();
			for (JsonElement point : points_array) {
//				System.out.println(point.toString());
				JsonArray entry_array = point.getAsJsonObject().get("row").getAsJsonObject().get("entry").getAsJsonArray();
				System.out.println(" ");
				JsonObject innerObject = new JsonObject();
//				innerObject.addProperty("name", "john");
				meas_array.add(innerObject);
				for (JsonElement entry : entry_array) {
					JsonObject kv_pair = entry.getAsJsonObject();
					System.out.println(kv_pair.toString());

					buildMeasurementObject(innerObject, entry);
				}
				String time = innerObject.get("timestamp").getAsString();
				innerObject.remove("timestamp");
				if(! temp_times.has(time) ){
					temp_times.add(time,new JsonArray());
				}
				temp_times.get(time).getAsJsonArray().add(innerObject);
			}
		}
		


		int index = 0;
		for (Entry<String, JsonElement> time_entry : temp_times.entrySet()) {
			simOutputObject.addProperty("timestame", time_entry.getKey());
			simOutputObject.add("measurements", time_entry.getValue());
			JsonObject msgObject = new JsonObject();
			msgObject.addProperty("simulation_id", simulationId);
			msgObject.add("message", simOutputObject);

			JsonObject outputObject = new JsonObject();
			outputObject.add("output", msgObject);
			System.out.println(simOutputObject.toString());
			TestResults tr = compareResults.compareExpectedWithSimulationOutput(index+"", outputObject, expected_output_series);
			if (tr != null) {
				testResultSeries.add(index+"", tr);
			}
			index++;
		}
		
		System.out.println("Index: " + index + " TestManager number of conflicts: "+ " total " + testResultSeries.getTotal());
	}

	public Set<String> getTimes(String responses) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

		Set<String> times = new HashSet<String>();
		JsonObject jsonObject = CompareResults.getSimulationJson(responses);
		JsonArray ma = jsonObject.get("measurements").getAsJsonArray();
		for (JsonElement meas : ma) {
			JsonArray points_array = meas.getAsJsonObject().get("points").getAsJsonArray();
			for (JsonElement point : points_array) {
				JsonArray entry_array = point.getAsJsonObject().get("row").getAsJsonObject().get("entry")
						.getAsJsonArray();
				for (JsonElement entry : entry_array) {
					String key = entry.getAsJsonObject().get("key").getAsString();
					if (key.equals("time")) {
						String value = entry.getAsJsonObject().get("value").getAsString();
						Date startTime = null;
						try {
							startTime = sdf.parse(value);
						} catch (ParseException e) {
							e.printStackTrace();
						}
						long startTime2 = startTime.getTime();
						// System.out.println(startTime2);
						times.add(startTime2 + "");
					}
				}
			}
		}
		return times;
	}

private ProvenResponse query(String simulationId, String hasMrid, String startTime, String endTime,  List<String> keywords) {
	//1532971828475
//	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
//	sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
//	try {
//		Date date = sdf.parse("2018-07-30T17:30:28.475Z");
//		System.out.println(date.getTime());
//	} catch (ParseException e1) {
//		// TODO Auto-generated catch block
//		e1.printStackTrace();
//	}
	
	ProvenProducer provenProducer = new ProvenProducer();
	provenProducer.restProducer(provenUri, null, null);
	provenProducer.setMessageInfo("GridAPPSD", "SimulationOutput", this.getClass().getSimpleName(), keywords);

	provenProducer.restProducer(provenUri, null, null);
	provenProducer.setMessageInfo("GridAPPSD", "QUERY", this.getClass().getSimpleName(), null);

	QueryFilter queryFilter = new QueryFilter();	
	queryFilter.hasSimulationId = simulationId;
	queryFilter.hasSimulationMessageType = "OUTPUT";
//	queryFilter.hasMrid = new String[]{"_e10b535c-79f3-498b-a38f-11d1cc50f3a0" , "_1a00c33f-ce5f-4e23-a141-dfda798927aa"};
//	queryFilter.hasMrid = "_e10b535c-79f3-498b-a38f-11d1cc50f3a0" ;
	if (hasMrid != null) queryFilter.hasMrid = hasMrid;
	if (startTime != null) queryFilter.startTime = startTime;
	if (endTime != null) queryFilter.endTime = endTime;
//	queryFilter.startTime = "1532971828475";
//	queryFilter.endTime = "1532971828475";
//	queryFilter.endTime = "1532971828495"; 
//		queryFilter.startTime = "2018-07-30 17:30:28.475";
//		queryFilter.endTime = "2018-07-30 17:30:28.475000+00:00";
	
//		2018-07-30T17:30:28.495

	ProvenQuery provenQuery = new ProvenQuery();
	provenQuery.queryFilter = queryFilter;
//	System.out.println(provenQuery.toString());
	ProvenResponse response = null;
	try {
		//'select "hasMrid", "hasValue", "hasMagnitude", "hasAngle" FROM "proven"."autogen"."PROVEN_MEASUREMENT" where time > now() - 100d AND "hasSimulationMessageType"=\'OUTPUT\'  LIMIT 8736'
//			response = provenProducer.sendMessage("SELECT \"hasMagnitude\" FROM \"proven\".\"autogen\".\"PROVEN_MEASUREMENT\" WHERE time > now() - 200h", "1");
		String q = "{\"queryMeasurement\":\"simulation\",\"queryType\":\"time-series\",\"queryFilter\":{\"hasSimulationId\":\"1259017289\"}}";
//			Map<String, JsonElement> expectedOutputMap = compareResults.getExpectedOutputMap("0", expected_output_series);
//			Set<String> keySet = expectedOutputMap.keySet();
//			for (String key : keySet) {
//				queryFilter.hasMrid = key;
//				response = provenProducer.sendMessage(provenQuery.toString(), null);
//			 	System.out.println(response.result.toString());
//			}
		response = provenProducer.sendMessage(provenQuery.toString(), null);
	
	} catch (InvalidProvenMessageException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (SendMessageException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (NullExchangeInfoException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	System.out.println(response.toString() );
	return response;
}
	

	private void mySend(String provenUri) {
		ProvenProducer provenProducer = new ProvenProducer();
		provenProducer.restProducer(provenUri, null, null);
		provenProducer.setMessageInfo("GridAPPSD", "SimulationOutput", this.getClass().getSimpleName(), null);
		
		String testMsg1 = "{\"output\": {\"simulation_id\": \"1278337149\", \"message\": {\"timestamp\": \"1532971828475\", \"measurements\": [{\"magnitude\": 2284.104442616845, \"angle\": -3.1188732475421252, \"measurement_mrid\": \"_8ddf47cd-f3d0-42b0-b6a4-5689a329d4b2\"}, {\"magnitude\": 2313.4540850870344, \"angle\": 118.78305214392333, \"measurement_mrid\": \"_e10b535c-79f3-498b-a38f-11d1cc50f3a0\"}, {\"magnitude\": 2324.8441688056346, \"angle\": -3.006374244440151, \"measurement_mrid\": \"_f997fdd0-0133-43a3-a5da-76381fba09b9\"}]}}} ";
		String testMsg2 = "{\"output\": {\"simulation_id\": \"1278337149\", \"message\": {\"timestamp\": \"1532971828495\", \"measurements\": [{\"magnitude\": 2284.104442616845, \"angle\": -3.1188732475421252, \"measurement_mrid\": \"_8ddf47cd-f3d0-42b0-b6a4-5689a329d4b2\"}, {\"magnitude\": 2313.4540850870344, \"angle\": 118.78305214392333, \"measurement_mrid\": \"_e10b535c-79f3-498b-a38f-11d1cc50f3a0\"}, {\"magnitude\": 2324.8441688056346, \"angle\": -3.006374244440151, \"measurement_mrid\": \"_f997fdd0-0133-43a3-a5da-76381fba09b9\"}]}}} ";
		try {
			provenProducer.sendMessage(testMsg1, null);
			provenProducer.sendMessage(testMsg2, null);
		} catch (InvalidProvenMessageException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SendMessageException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NullExchangeInfoException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	

	public static void main(String[] args) {
		ProvenQueryTestManager2 pq = new ProvenQueryTestManager2();
		String expected_output_series = "/Users/jsimpson/git/adms/gridappsd-sample-app/sample_app/tests/expected_result_series_123_full.json";
		pq.test_proven("1278337149", expected_output_series);
	}
	
	class ProvenQuery implements Serializable{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 121333333L;
		String queryMeasurement = "simulation";
		String queryType = "time-series"; 
		QueryFilter queryFilter;	
		
		@Override
		public String toString() {
			Gson  gson = new Gson();
			return gson.toJson(this);
		}
 
	}

	class QueryFilter implements Serializable{
		/**
		 *  
		 */
		private static final long serialVersionUID = 12312312L;
			String hasSimulationId;
		    String hasSimulationMessageType;
		    String hasMrid; 
		    String startTime;
		    String endTime;
		    
		    @Override
			public String toString() { 
				Gson  gson = new Gson();
				return gson.toJson(this);
			}
	}
}
	
	

