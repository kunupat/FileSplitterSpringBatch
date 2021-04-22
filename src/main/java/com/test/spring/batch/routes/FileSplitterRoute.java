package com.test.spring.batch.routes;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class FileSplitterRoute extends RouteBuilder {

	@Override
	public void configure() throws Exception {
		System.out.println("Configuring camel routes...");
		
		String inputFilePath = System.getProperty("user.home") + "/data/input/" ;
		String outputFilePath = System.getProperty("user.home") + "/data/out/" ;
		
		from("direct:start")
		.noAutoStartup()
		.pollEnrich("file://" + inputFilePath + "?noop=true&fileName=customers.csv")
	    .split().tokenize("\n",100)
	    .to("file://" + outputFilePath + "?fileName=customers_"+ "${header.CamelSplitIndex}" + ".csv")
	    .routeId("fileSplitterRoute")
	    .end();
	    
	    System.out.println("Configured camel routes...");
	}
}
