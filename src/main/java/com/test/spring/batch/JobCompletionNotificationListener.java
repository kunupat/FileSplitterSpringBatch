package com.test.spring.batch;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;

import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {
	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("!!! JOB FINISHED! Time to verify the results");

			String outputFilePath = System.getProperty("user.home") + "/data/out/";

			File file = new File(outputFilePath);
			File fileList[] = file.listFiles(new FilenameFilter() {

				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".csv");
				}

			});

			JSONArray jsArray = new JSONArray(Arrays.asList(fileList));
			
			log.info("Split File List: " + jsArray.toString());
			
			jobExecution.setExitStatus(new ExitStatus("fileList", jsArray.toString()));
		}
	}
}