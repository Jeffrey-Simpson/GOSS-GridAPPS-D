package gov.pnnl.goss.gridappsd.testmanager;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;


import gov.pnnl.proven.api.exception.NullExchangeInfoException;
import gov.pnnl.proven.api.exception.SendMessageException;
import gov.pnnl.proven.api.exchange.ExchangeInfo;
import gov.pnnl.proven.api.exchange.ExchangeType;
import gov.pnnl.proven.api.producer.ProvenProducer;
import gov.pnnl.proven.api.producer.MessageInfo;
import gov.pnnl.proven.api.producer.Producer;
import gov.pnnl.proven.api.producer.ProvenResponse;
import gov.pnnl.proven.message.ProvenMeasurement;
import gov.pnnl.proven.message.ProvenMessage;
import gov.pnnl.proven.message.ProvenStatement;
import gov.pnnl.proven.message.exception.InvalidProvenMessageException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
//import org.topbraid.shacl.rules.RuleUtil;
import org.apache.jena.rdf.model.Model;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.internal.RuntimeDelegateImpl;

public class ProvenQueryTestManager {
	String provenUri = "http://localhost:18080/hybrid/rest/v1/repository/provenMessage";

	public void test_proven(){
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
		
//		JerseyClientBuilder jcb = new JerseyClientBuilder();
//		RuntimeDelegateImpl x = new RuntimeDelegateImpl();
		
		QueryFilter queryFilter = new QueryFilter();
//		queryFilter.hasSimulationId = "1259017289";
//		queryFilter.hasSimulationMessageType = "OUTPUT";
////		queryFilter.hasMrid = "04002c33-d589-4f21-89ce-3e09e0d98e";
//		queryFilter.startTime = "2018-07-24 09:40:57.00000+00:00";
//		queryFilter.endTime = "2018-07-29 19:40:57.00000+00:00";
////		queryFilter.endTime = "2018-07-27 15:55:57.196000+00:00";
		
//		queryFilter.hasSimulationId = "1278337149";
		queryFilter.hasSimulationMessageType = "OUTPUT";
//		queryFilter.hasMrid = "_e10b535c-79f3-498b-a38f-11d1cc50f3a0";
//		queryFilter.startTime = "2018-07-30 17:30:28.475000+00:00";

		ProvenQuery provenQuery = new ProvenQuery();
		provenQuery.queryFilter = queryFilter;
		System.out.println(provenQuery.toString());
		ProvenResponse response = null;
		try {
			//'select "hasMrid", "hasValue", "hasMagnitude", "hasAngle" FROM "proven"."autogen"."PROVEN_MEASUREMENT" where time > now() - 100d AND "hasSimulationMessageType"=\'OUTPUT\'  LIMIT 8736'
//			response = provenProducer.sendMessage("SELECT \"hasMagnitude\" FROM \"proven\".\"autogen\".\"PROVEN_MEASUREMENT\" WHERE time > now() - 200h", "1");
			String q = "{\"queryMeasurement\":\"simulation\",\"queryType\":\"time-series\",\"queryFilter\":{\"hasSimulationId\":\"1259017289\"}}";
			
			q = "{\"queryMeasurement\":\"simulation\",\"queryType\":\"timeseries\",\"queryFilter\":{\"hasSimulationId\":\"1278337149\"}}";
			
//			response = sendMessage(q, null);
			

			response = provenProducer.sendMessage(provenQuery.toString(), null);
//			response = sendMessage(provenQuery.toString(), null);
//			((Producer) provenProducer).;
			
			System.out.println(provenProducer.messageFailCount());
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
		
	}
	
	public ProvenResponse sendMessage(String message, String requestId) throws InvalidProvenMessageException, SendMessageException, NullExchangeInfoException {
		MessageInfo messageInfo = new MessageInfo("GridAPPSD", "SimulationOutput", this.getClass().getSimpleName(), null);
		ExchangeInfo exchangeInfo = new ExchangeInfo(ExchangeType.REST, provenUri, null, null);
		ProvenProducer provenProducer = new ProvenProducer();
		provenProducer.restProducer(provenUri, null, null);
		provenProducer.setMessageInfo("GridAPPSD", "QUERY", this.getClass().getSimpleName(), null);
		
		ProvenMessage pm;
		if(messageInfo != null) {			
			pm = ProvenMessage.message(message).keywords(messageInfo.getKeywords()).domain(messageInfo.getDomain()).name(messageInfo.getName()).source(messageInfo.getSource()).build();
		}
		else
			pm = ProvenMessage.message(message).build();

		 ProvenResponse pr = provenProducer.sendMessage(pm, exchangeInfo, requestId);
		 
		 System.out.println(pr.toString());
		 System.out.println(pm.getStatements().size());
		 
		 for (ProvenStatement meas : pm.getStatements() ) {
			 System.out.println(meas.toString());
			 System.out.println(meas.getSubject());
//			 ObjectDataInput in = null;
//			 try {
//				meas.readData(in);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
		 for (ProvenMeasurement meas : pm.getMeasurements()) {
			 System.out.println(meas.toString());
		}
		 return pr;
	}


	private void mySend(String provenUri) {
		ProvenProducer provenProducer = new ProvenProducer();
		provenProducer.restProducer(provenUri, null, null);
		String message = "{\"output\": {\"simulation_id\": \"1278337149\", \"message\": {\"timestamp\": \"1532971828475\", \"measurements\": [{\"magnitude\": 2284.104442616845, \"angle\": -3.1188732475421252, \"measurement_mrid\": \"_8ddf47cd-f3d0-42b0-b6a4-5689a329d4b2\"}, {\"magnitude\": 2313.4540850870344, \"angle\": 118.78305214392333, \"measurement_mrid\": \"_e10b535c-79f3-498b-a38f-11d1cc50f3a0\"}, {\"magnitude\": 2324.8441688056346, \"angle\": -3.006374244440151, \"measurement_mrid\": \"_f997fdd0-0133-43a3-a5da-76381fba09b9\"}]}}} ";
	
		ExchangeInfo exchangeInfo = new ExchangeInfo(ExchangeType.REST, provenUri, null, null);
		
		try {
			ProvenMessage pm = ProvenMessage.message(message).build();
			pm.getMeasurements();
			Entity<ProvenMessage> em = Entity.entity(pm, MediaType.APPLICATION_JSON);
			System.out.println(em.toString());
		} catch (InvalidProvenMessageException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

	
//		return sendMessage(pm, exchangeInfo, null);
		
		try {
			provenProducer.sendMessage(message, null);
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
		ProvenQueryTestManager pq = new ProvenQueryTestManager();
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
	
	

