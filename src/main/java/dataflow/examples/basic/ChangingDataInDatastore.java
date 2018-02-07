package dataflow.examples.basic;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import dataflow.examples.DataflowExampleOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Map;

class ChangingDataInDatastore {

    public static void main(String[] args) {
        DataflowExampleOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DataflowExampleOptions.class);
        String projectId = options.getProject();

        Query.Builder queryBuilder = Query.newBuilder();
        queryBuilder.addKindBuilder().setName("dataflow-examples-person").build();
        Query query = queryBuilder.build();

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Read from Datastore", DatastoreIO.v1().read()
                        .withProjectId(projectId)
                        .withQuery(query))
                .apply("To Entity", MapElements.into(TypeDescriptor.of(Entity.class))
                        .via(ChangingDataInDatastore::toPersonV2))
                .apply("Write to Datastore", DatastoreIO.v1().write()
                        .withProjectId(projectId));

        pipeline.run();
    }

    private static Entity toPersonV2(Entity old) {

        // split name into two parts: firstName and lastName
        Map<String, Value> propsMap = old.getPropertiesMap();
        String[] nameSplits = propsMap.get("name").getStringValue().split(" ");
        String firstName = nameSplits[0];
        String lastName = nameSplits[1];

        Key.PathElement oldKeyPath = old.getKey().getPath(0);
        return Entity.newBuilder(old)
                .setKey(Key.newBuilder()
                        .addPath(Key.PathElement.newBuilder()
                                .setName(oldKeyPath.getName())
                                .setKind("dataflow-examples-person-v2")
                                .build())
                        .build())
                .removeProperties("name")
                .putProperties("firstName", Value.newBuilder().setStringValue(firstName).build())
                .putProperties("lastName", Value.newBuilder().setStringValue(lastName).build())
                .build();
    }
}
