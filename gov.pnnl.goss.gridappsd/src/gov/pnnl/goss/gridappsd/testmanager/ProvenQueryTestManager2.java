package gov.pnnl.goss.gridappsd.testmanager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gov.pnnl.proven.api.exception.NullExchangeInfoException;
import gov.pnnl.proven.api.exception.SendMessageException;
import gov.pnnl.proven.api.producer.ProvenProducer;
import gov.pnnl.proven.api.producer.ProvenResponse;
import gov.pnnl.proven.message.exception.InvalidProvenMessageException;

public class ProvenQueryTestManager2 {
	String provenUri = "http://localhost:18080/hybrid/rest/v1/repository/provenMessage";
	
	
	public String getListOfTime(String expected_output_series){
		CompareResults compareResults = new CompareResults();
		Map<String, JsonElement> expectedOutputMap = compareResults.getExpectedOutputMap("0", expected_output_series);
		Set<String> keySet = expectedOutputMap.keySet();
		String first = keySet.iterator().next();
		return null;
		
	}
	
//	public String query(){
//		
//	}

	public void test_proven(){
		CompareResults compareResults = new CompareResults();
		TestResultSeries testResultSeries = new TestResultSeries();
//		String expected_output_series = "/home/gridappsd/gridappsd_project/sources/GOSS-GridAPPS-D/gov.pnnl.goss.gridappsd/test/gov/pnnl/goss/gridappsd/expected_output_series3.json";
		String expected_output_series = "/Users/jsimpson/git/adms/gridappsd-sample-app/sample_app/tests/expected_result_series_123_full.json";
		
//		ProvenTimeSeriesDataManagerImpl pm = new ProvenTimeSeriesDataManagerImpl();
//		RequestTimeseriesData rs = new  RequestTimeseriesData();
		List<String> keywords = new ArrayList<String> ();
		keywords.add("hasValue");

//		provenUri = "http://proven:8080/hybrid/rest/v1/repository/provenMessage";
		
		ProvenProducer provenProducer = new ProvenProducer();
		provenProducer.restProducer(provenUri, null, null);
		provenProducer.setMessageInfo("GridAPPSD", "SimulationOutput", this.getClass().getSimpleName(), keywords);
		
//		mySend(provenUri);

		provenProducer.restProducer(provenUri, null, null);
		provenProducer.setMessageInfo("GridAPPSD", "QUERY", this.getClass().getSimpleName(), null);
		String simulationId = "1278337149";
		QueryFilter queryFilter = new QueryFilter();	
		queryFilter.hasSimulationId = simulationId;
		queryFilter.hasSimulationMessageType = "OUTPUT";
//		queryFilter.hasMrid = new String[]{"_e10b535c-79f3-498b-a38f-11d1cc50f3a0" , "_1a00c33f-ce5f-4e23-a141-dfda798927aa"};
		queryFilter.hasMrid = "_e10b535c-79f3-498b-a38f-11d1cc50f3a0" ;
		queryFilter.startTime = "1532971828475";
		queryFilter.endTime = "1532971828475";
		queryFilter.endTime = "1532971828495"; 
//		queryFilter.startTime = "2018-07-30 17:30:28.475";
//		queryFilter.endTime = "2018-07-30 17:30:28.475000+00:00";
		
//		2018-07-30T17:30:28.495

		ProvenQuery provenQuery = new ProvenQuery();
		provenQuery.queryFilter = queryFilter;
		System.out.println(provenQuery.toString());
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
			System.out.println(response.result.toString());
			
			
			
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
		System.out.println(provenQuery.toString());
		System.out.println(response.toString() );
		
//		System.out.println(response.result.toString());
		JsonObject jsonObject = CompareResults.getSimulationJson(response.result.toString());
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

					String key = entry.getAsJsonObject().get("key").getAsString();
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
	

	private void mySend(String provenUri) {
		ProvenProducer provenProducer = new ProvenProducer();
		provenProducer.restProducer(provenUri, null, null);
		provenProducer.setMessageInfo("GridAPPSD", "SimulationOutput", this.getClass().getSimpleName(), null);
		
//		String testMsg1 = "{\"output\": {\"simulation_id\": \"1278337149\", \"message\": {\"timestamp\": \"1532971828475\", \"measurements\": [{\"magnitude\": 2284.104442616845, \"angle\": -3.1188732475421252, \"measurement_mrid\": \"_8ddf47cd-f3d0-42b0-b6a4-5689a329d4b2\"}, {\"magnitude\": 2313.4540850870344, \"angle\": 118.78305214392333, \"measurement_mrid\": \"_e10b535c-79f3-498b-a38f-11d1cc50f3a0\"}, {\"magnitude\": 2324.8441688056346, \"angle\": -3.006374244440151, \"measurement_mrid\": \"_f997fdd0-0133-43a3-a5da-76381fba09b9\"}]}}} ";
		String testMsg2 = "{\"output\": {\"simulation_id\": \"1278337149\", \"message\": {\"timestamp\": \"1532971828495\", \"measurements\": [{\"magnitude\": 2284.104442616845, \"angle\": -3.1188732475421252, \"measurement_mrid\": \"_8ddf47cd-f3d0-42b0-b6a4-5689a329d4b2\"}, {\"magnitude\": 2313.4540850870344, \"angle\": 118.78305214392333, \"measurement_mrid\": \"_e10b535c-79f3-498b-a38f-11d1cc50f3a0\"}, {\"magnitude\": 2324.8441688056346, \"angle\": -3.006374244440151, \"measurement_mrid\": \"_f997fdd0-0133-43a3-a5da-76381fba09b9\"}]}}} ";
		try {
//			provenProducer.sendMessage(testMsg1, null);
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
		pq.test_proven();
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
	
	

