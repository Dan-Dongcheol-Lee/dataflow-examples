package dataflow.examples.template;

import com.google.api.services.dataflow.model.CreateJobFromTemplateRequest;
import com.google.api.services.dataflow.model.Job;
import dataflow.examples.client.DataflowClient;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.util.HashMap;
import java.util.Map;

import static java.lang.Thread.sleep;

public class CsvToDatastoreClient {

    public static void main(String[] args) throws InterruptedException {
        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);

        String projectId = options.getProject();
        String jobName = options.getJobName();
        String templateLocation = options.getTemplateLocation();
        Map<String, String> params = new HashMap<>();
        params.put("csvFilePath", "gs://"+projectId+".appspot.com/dataflow-examples/csv/seed-person-1000.csv");
        params.put("datastoreEntity", "dataflow-example-person-from-csv-1000");

        System.out.println("\n* parameters: " + params);
        sleep(5000L);

        CreateJobFromTemplateRequest content = new CreateJobFromTemplateRequest()
                .setGcsPath(templateLocation)
                .setParameters(params);

        Job job = DataflowClient.executeJob(
                projectId,
                jobName,
                content);

        System.out.println("* job: " + job);
    }
}
