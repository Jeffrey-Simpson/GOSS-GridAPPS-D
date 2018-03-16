package gov.pnnl.goss.gridappsd.testmanager;

import org.apache.felix.dm.annotation.api.Component;
import org.apache.felix.dm.annotation.api.ServiceDependency;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;

import gov.pnnl.goss.cim2glm.components.DistCapacitor;
import gov.pnnl.goss.cim2glm.components.DistCoordinates;
import gov.pnnl.goss.cim2glm.queryhandler.QueryHandler;
import gov.pnnl.goss.gridappsd.api.ConfigurationManager;
import gov.pnnl.goss.gridappsd.data.handlers.BlazegraphQueryHandler;
import gov.pnnl.goss.gridappsd.utils.GridAppsDConstants;

@Component
public class TestManagerQueryFactory {
	@ServiceDependency
	private volatile ConfigurationManager configurationManager;
	
	public TestManagerQueryFactory(){}
	public TestManagerQueryFactory(ConfigurationManager configurationManager){
		this.configurationManager = configurationManager;
	}
	

	public String getFeeder() {
		
		
		QueryHandler queryHandler = getQueryHandler();
		
//		ResultSet results1 = queryHandler.query (DistCapacitor.szQUERY);

//		ResultSet results2 = queryHandler.query (DistCoordinates.szQUERY);
		
		String lineQuery = "SELECT ?name WHERE { " + "?s r:type c:Line. " + "?s c:IdentifiedObject.name ?name} "
				+ "ORDER by ?name";
		ResultSet results = queryHandler.query(lineQuery);
		String feederName = getResultName(results);
		return feederName;
	}
	
	public String getGeographicalRegion() {
		QueryHandler queryHandler = getQueryHandler();

		String geoRegionQuery = "SELECT ?name WHERE { " + "?s r:type c:GeographicalRegion. " + "?s c:IdentifiedObject.name ?name} "
				+ "ORDER by ?name";
		ResultSet results = queryHandler.query(geoRegionQuery);
		String geoName = getResultName(results);
		return geoName;
	}
	
	public String getSubGeographicalRegion() {
		QueryHandler queryHandler = getQueryHandler();
		String subGeoRegionQuery = "SELECT ?name WHERE { " + "?s r:type c:SubGeographicalRegion. " + "?s c:IdentifiedObject.name ?name} "
				+ "ORDER by ?name";
		ResultSet results = queryHandler.query(subGeoRegionQuery);
		String subGeoName = getResultName(results);	
		return subGeoName;
	}
	
	public QueryHandler getQueryHandler() {		
		String bgHost = configurationManager.getConfigurationProperty(GridAppsDConstants.BLAZEGRAPH_HOST_PATH);
		if(bgHost==null || bgHost.trim().length()==0){
			bgHost = "http://localhost:9999/blazegraph";
//			bgHost = "http://blazegraph:8080/bigdata";
		}

//		System.out.println("Blazegraph "+ bgHost);
		QueryHandler queryHandler = new BlazegraphQueryHandler(bgHost+"/namespace/kb/sparql");
		return queryHandler;
	}
	
	public String getResultName(ResultSet results) {
		 String feederName = "";
		if (results.hasNext()) {
			QuerySolution soln = results.next();
			feederName = soln.get("?name").toString();
//			System.out.println(feederName);
		}
		return feederName;
	}
	
}
