package dataflow.examples.multibranches;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class TwoDatastoresToMultiSinks {

    public static void main(String[] args) {

        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        Query.Builder queryBuilder = Query.newBuilder();
        queryBuilder.addKindBuilder()
                .setName("UpdatezipVersion")
                .build();

        PCollection<Entity> entities = pipeline.apply("Read from Datastore 1",
                DatastoreIO.v1().read()
                        .withProjectId(options.getProject())
                        .withQuery(queryBuilder.build()));

        entities
                .apply("To TableRows Raw", MapElements.into(TypeDescriptor.of(TableRow.class))
                        .via(TwoDatastoresToMultiSinks::toTableRow))
                .apply("Filter TableRows", Filter.by(Objects::nonNull))
                .apply("Write to BigQuery", BigQueryIO.<TableRow>writeTableRows()
                        .withJsonSchema(getTableSchemaJson())
                        .to(new TableReference()
                                .setProjectId(options.getProject())
                                .setDatasetId("ex_update")
                                .setTableId("ex_updateversion"))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND));

        entities
                .apply("To TableRows Flat", MapElements.into(TypeDescriptor.of(TableRow.class))
                        .via(TwoDatastoresToMultiSinks::toTableRow))
                .apply("Filter TableRows", Filter.by(Objects::nonNull))
                .apply("Write to BigQuery", BigQueryIO.<TableRow>writeTableRows()
                        .withJsonSchema(getTableSchemaJson())
                        .to(new TableReference()
                                .setProjectId(options.getProject())
                                .setDatasetId("ex_update")
                                .setTableId("ex_updateversion_flat1"))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND));


        Query.Builder queryBuilder2 = Query.newBuilder();
        queryBuilder2.addKindBuilder()
                .setName("InstallerVersion")
                .build();

        PCollection<Entity> entities2 = pipeline.apply("Read from Datastore 2",
                DatastoreIO.v1().read()
                        .withProjectId(options.getProject())
                        .withQuery(queryBuilder2.build()));

        entities2
                .apply("To TableRows Raw", MapElements.into(TypeDescriptor.of(TableRow.class))
                        .via(TwoDatastoresToMultiSinks::toTableRow))
                .apply("Filter TableRows", Filter.by(Objects::nonNull))
                .apply("Write to BigQuery", BigQueryIO.<TableRow>writeTableRows()
                        .withJsonSchema(getTableSchemaJson())
                        .to(new TableReference()
                                .setProjectId(options.getProject())
                                .setDatasetId("ex_update")
                                .setTableId("ex_installerversion"))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND));


        Query.Builder queryBuilder3 = Query.newBuilder();
        queryBuilder3.addKindBuilder()
                .setName("UpdatezipOperations")
                .build();

        PCollection<Entity> entities3 = pipeline.apply("Read from Datastore 3",
                DatastoreIO.v1().read()
                        .withProjectId(options.getProject())
                        .withQuery(queryBuilder3.build()));

        entities3
                .apply("To TableRows Raw", MapElements.into(TypeDescriptor.of(TableRow.class))
                        .via(TwoDatastoresToMultiSinks::toTableRow3))
                .apply("Write to BigQuery", BigQueryIO.<TableRow>writeTableRows()
                        .withJsonSchema(getTableSchemaJson3())
                        .to(new TableReference()
                                .setProjectId(options.getProject())
                                .setDatasetId("ex_update")
                                .setTableId("ex_update_operations"))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND));

        pipeline.run();
    }

    private static String getTableSchemaJson() {
        return "{\"fields\":[{\"name\": \"fileName\", \"type\": \"STRING\"}," +
                "{\"name\": \"platform\", \"type\": \"STRING\"}," +
                "{\"name\": \"sha256\",   \"type\": \"STRING\"}," +
                "{\"name\": \"vendor\",   \"type\": \"STRING\"}," +
                "{\"name\": \"version\",  \"type\": \"STRING\"}]}";
    }

    private static TableRow toTableRow(Entity entity) {

        String keyName = entity.getKey().getPath(0).getName();
        if (keyName == null || keyName.equals("")) {
            return null;
        }

        Map<String, Value> props = entity.getPropertiesMap();
        String fileName = getString(props, "FileName");
        String platform = getString(props, "Platform");
        String sha256 = getString(props, "Sha256");
        String vendor = getString(props, "Vendor");
        String version = getString(props, "Version");

        return new TableRow()
                .set("fileName", fileName)
                .set("platform", platform)
                .set("sha256", sha256)
                .set("vendor", vendor)
                .set("version", version);
    }

    private static String getTableSchemaJson3() {
        return "{\"fields\":[" +
                "{\"name\": \"action\", \"type\": \"STRING\"}," +
                "{\"name\": \"args\", \"type\": \"STRING\", \"mode\": \"REPEATED\"}," +
                "{\"name\": \"updatedTime\", \"type\": \"STRING\"}," +
                "{\"name\": \"vendor\", \"type\": \"STRING\"}]}";
    }

    private static TableRow toTableRow3(Entity entity) {
        Map<String, Value> props = entity.getPropertiesMap();
        String action = getString(props, "Action");
        ArrayValue args = props.get("Args").getArrayValue();
        String updatedTime = getString(props, "UpdatedTime");
        String vendor = getString(props, "Vendor");

        TableRow row = new TableRow()
                .set("action", action)
                .set("updatedTime", updatedTime)
                .set("vendor", vendor);

        if (args.getValuesCount() > 0) {
            List<String> argVals = args.getValuesList().stream()
                    .map(Value::getStringValue)
                    .collect(Collectors.toList());
            row.set("args", argVals);
        }

        return row;
    }

    private static String getString(Map<String, Value> props, String key) {
        return props.get(key) != null ? props.get(key).getStringValue() : null;
    }
}