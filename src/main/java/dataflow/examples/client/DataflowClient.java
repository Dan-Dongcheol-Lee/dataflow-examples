package dataflow.examples.client;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.DataflowRequestInitializer;
import com.google.api.services.dataflow.model.CreateJobFromTemplateRequest;
import com.google.api.services.dataflow.model.Job;
import com.google.auth.oauth2.ServiceAccountCredentials;

import java.io.IOException;

public final class DataflowClient {

    public static Job executeJob(String projectId,
                                 String jobName,
                                 CreateJobFromTemplateRequest content) {
        try {
            return dataflowClient(projectId)
                    .projects()
                    .templates()
                    .create(projectId, content)
                    .set("jobName", jobName)
                    .setAccessToken(ServiceAccountCredentials
                            .getApplicationDefault()
                            .refreshAccessToken()
                            .getTokenValue())
                    .execute();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Couldn't execute a job template. projectId: " + projectId
                            + ", jobName: " + jobName + ", content: " + content, e);
        }
    }

    private static Dataflow dataflowClient(String projectId) {
        return new Dataflow.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(), null)
                .setApplicationName(projectId)
                .setDataflowRequestInitializer(new DataflowRequestInitializer())
                .build();
    }
}
