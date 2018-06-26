package dataflow.examples.fanout;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Lists;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class DatastoreToMultiSinks {

    public static void main(String[] args) {

        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);

        String projectId = options.getProject();
        Query.Builder queryBuilder = Query.newBuilder();
        queryBuilder.addKindBuilder()
                .setName("UpdatezipVersion")
                .build();

        Pipeline pipeline = Pipeline.create(options);
        PCollection<Entity> entities = pipeline.apply("Read from Datastore",
                DatastoreIO.v1().read()
                        .withProjectId(options.getProject())
                        .withQuery(queryBuilder.build()));

        entities
                .apply("To TableRows Raw", MapElements.into(TypeDescriptor.of(TableRow.class))
                        .via(DatastoreToMultiSinks::toTableRow))
                .apply("Filter TableRows", Filter.by(Objects::nonNull))
                .apply("Write to BigQuery", BigQueryIO.<TableRow>writeTableRows()
                        .withJsonSchema(getTableSchemaJson())
                        .to(new TableReference()
                                .setProjectId(projectId)
                                .setDatasetId("ex_update")
                                .setTableId("ex_updateversion"))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND));

        entities
                .apply("To TableRows Flat", MapElements.into(TypeDescriptor.of(TableRow.class))
                        .via(DatastoreToMultiSinks::toTableRow))
                .apply("Filter TableRows", Filter.by(Objects::nonNull))
                .apply("Write to BigQuery", BigQueryIO.<TableRow>writeTableRows()
                        .withJsonSchema(getTableSchemaJson())
                        .to(new TableReference()
                                .setProjectId(projectId)
                                .setDatasetId("ex_update")
                                .setTableId("ex_updateversion_flat1"))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND));

        entities
                .apply("To Datastore Entity", MapElements
                        .into(TypeDescriptors.lists(TypeDescriptor.of(Entity.class)))
                        .via(DatastoreToMultiSinks::toEntities))
                .apply("Flatten", Flatten.iterables())
                .apply("Write to DataStore", DatastoreIO.v1().write()
                        .withProjectId(projectId));

        pipeline.run();
    }


    static List<Entity> toEntities(Entity entityVal) {
        String keyName = entityVal.getKey().getPath(0).getName();
        if (keyName == null || keyName.equals("")) {
            return new ArrayList<>();
        }

        Entity entity1 = Entity.newBuilder(entityVal)
                .setKey(Key.newBuilder()
                        .addPath(Key.PathElement.newBuilder()
                                .setKind("example-UpdatezipVersion-1")
                                .setName(keyName)
                                .build())
                        .build())
                .build();

        Entity entity2 = Entity.newBuilder(entityVal)
                .setKey(Key.newBuilder()
                        .addPath(Key.PathElement.newBuilder()
                                .setKind("example-UpdatezipVersion-2")
                                .setName(keyName)
                                .build())
                        .build())
                .build();

        return Lists.newArrayList(entity1, entity2);
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

    private static String getString(Map<String, Value> props, String key) {
        return props.get(key) != null ? props.get(key).getStringValue() : null;
    }
}