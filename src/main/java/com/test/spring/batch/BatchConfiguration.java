package com.test.spring.batch;

import org.apache.camel.ProducerTemplate;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.test.spring.batch.cloud.aws.s3.SpringCloudS3;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	private SpringCloudS3 springCloudS3;

	@Autowired
	private ProducerTemplate producerTemplate;
	
	@Bean
	public Tasklet downloadS3FileTask() {
		Tasklet tasklet = new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				springCloudS3.downloadS3Object("s3://spring-batch-test-bucket/customers.csv");
				return RepeatStatus.FINISHED;
			}
		};

		return tasklet;
	}

	@Bean
	public Tasklet splitFileTask() {
		Tasklet tasklet = new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Starting tasklet to split files...");
				
				//1. Start routes (or startAllRoutes())
				producerTemplate.getCamelContext().startRoute("fileSplitterRoute");
				
				//2. Send message to start the direct route
				producerTemplate.sendBody("direct:start", "");
				
				return RepeatStatus.FINISHED;
			}
		};
		return tasklet;
	}

	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
		return jobBuilderFactory
				.get("FileDownloaderAndSplitterJob")
				.incrementer(new RunIdIncrementer())
				.flow(step1())
				.next(step2())
				.end().listener(listener)
				.build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory
				.get("Step-1-Download-File-From-S3")
				.tasklet(downloadS3FileTask())
				.build();
	}

	@Bean
	public Step step2() {
		return stepBuilderFactory
				.get("Step-2-split-file")
				.tasklet(splitFileTask())
				.build();
	}
}