package dataflow.examples.basic;

import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import dataflow.examples.DataflowExampleOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Map;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

class MigratingToBigQuery {

    public static void main(String[] args) {
        DataflowExampleOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DataflowExampleOptions.class);
        String projectId = options.getProject();

        Query.Builder queryBuilder = Query.newBuilder();
        queryBuilder.addKindBuilder()
                .setName("dataflow-examples-person")
                .build();
        Query query = queryBuilder.build();

        Pipeline pipeline = Pipeline.create(options);
        PCollection<Entity> persons = pipeline
                .apply("Read from Datastore",
                        DatastoreIO.v1().read()
                                .withProjectId(projectId)
                                .withQuery(query));

        persons.apply("To TableRow", MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(MigratingToBigQuery::toTableRow))
                .apply("Write to BigQuery",
                        BigQueryIO.writeTableRows()
                                .withJsonSchema(getTableSchemaJson())
                                .to(input -> new TableDestination(
                                        projectId + ":dataflow_examples.person", null))
                                .withCreateDisposition(CREATE_IF_NEEDED)
                                .withWriteDisposition(WRITE_APPEND));

        persons.apply("To TableRow v2", MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(MigratingToBigQuery::toTableRowV2))
                .apply("Write to BigQuery v2",
                        BigQueryIO.writeTableRows()
                                .withJsonSchema(getTableSchemaJsonV2())
                                .to(input -> new TableDestination(
                                        projectId + ":dataflow_examples.person_v2", null))
                                .withCreateDisposition(CREATE_IF_NEEDED)
                                .withWriteDisposition(WRITE_APPEND));

        pipeline.run();
    }

    private static String getTableSchemaJson() {
        return "{\"fields\":[" +
                "{\"name\": \"name\", \"type\": \"STRING\"}," +
                "{\"name\": \"address\", \"type\": \"STRING\"}" +
                "]}";
    }

    public static TableRow toTableRow(Entity entity) {
        Map<String, Value> propsMap = entity.getPropertiesMap();
        return new TableRow()
                .set("name", propsMap.get("name").getStringValue())
                .set("address", propsMap.get("address").getStringValue());
    }

    private static String getTableSchemaJsonV2() {
        return "{\"fields\":[" +
                "{\"name\": \"firstName\", \"type\": \"STRING\"}," +
                "{\"name\": \"lastName\", \"type\": \"STRING\"}," +
                "{\"name\": \"address\", \"type\": \"STRING\"}," +
                "{\"name\": \"suburb\", \"type\": \"STRING\"}" +
                "]}";
    }

    public static TableRow toTableRowV2(Entity entity) {
        Map<String, Value> propsMap = entity.getPropertiesMap();

        String[] nameSplits = propsMap.get("name").getStringValue().split(" ");
        String firstName = nameSplits[0].trim();
        String lastName = nameSplits[1].trim();
        String[] addressSplits = propsMap.get("address").getStringValue().split(",");
        String suburb = addressSplits[addressSplits.length - 2];

        return new TableRow()
                .set("firstName", firstName.trim())
                .set("lastName", lastName.trim())
                .set("address", propsMap.get("address").getStringValue().trim())
                .set("suburb", suburb.trim());
    }
}
