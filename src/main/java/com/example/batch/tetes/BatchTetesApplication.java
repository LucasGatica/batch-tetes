package com.example.batch.tetes;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;


@SpringBootApplication
public class BatchTetesApplication {


	public static void main(String[] args) {
		SpringApplication.run(BatchTetesApplication.class, args);  }




	@Bean
	Tasklet    tasklet(){
		return (contribution, chunkContext) ->{
			System.out.println("Hello world");
			return RepeatStatus.FINISHED;
		};
	}


	record CsvRow(String title,String author,String link, String bookshelf){


	}
	@Bean
	FlatFileItemReader<CsvRow> csvRowFlatFileItemReader(
			@Value("file:/Users/lucasgatica/Downloads/batch-tetes/src/main/resources/gutenberg_metadata.csv") Resource resource){


		return new FlatFileItemReaderBuilder<CsvRow>()

				.resource(resource)
				.name("csv")

				.delimited().delimiter(",")
				.names("title,author,link,bookshelf".split(","))
				.linesToSkip(1)
				.fieldSetMapper(fieldSet ->
						new CsvRow(

								fieldSet.readString(0),
								fieldSet.readString(1),
								fieldSet.readString(2),
								fieldSet.readString(3)


						)).build();


	}






	@Bean
	Step csvToDb (JobRepository jobRepository, PlatformTransactionManager txm,FlatFileItemReader<CsvRow> csvRowFlatFileItemReader) throws Exception{




		return new StepBuilder("csvToDb", jobRepository)
				.<CsvRow, CsvRow>chunk(100, txm)
				.reader(csvRowFlatFileItemReader)
				.writer(chunk -> {
					var oneHundredRows = chunk.getItems();
					System.out.println("teste"+oneHundredRows);
				})
				.build();
	}


	@Bean
	Job job(JobRepository jobRepository, Step step, Step csvToDb){
		return new JobBuilder("job",jobRepository)
				.start(step)
				.next(csvToDb)
				.build();
	}


	@Bean
	Step step(JobRepository jobRepository, Tasklet tasklet, PlatformTransactionManager transactionManager){
		return new StepBuilder("step", jobRepository)
				.tasklet(tasklet,transactionManager)
				.build();


	}
}

