package dataflow.examples.template;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.UUID;

class CsvToDatastore {

    public static void main(String[] args) {
        CsvToDatastoreTemplateOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(CsvToDatastoreTemplateOptions.class);

        String projectId = options.getProject();

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Read from CSV", TextIO.read().from(options.getCsvFilePath()))
                .apply("To Entity", ParDo.of(new ToEntityFn()))
                .apply("Write to Datastore", DatastoreIO.v1().write().withProjectId(projectId));

        pipeline.run();
    }

    static class ToEntityFn extends DoFn<String, Entity> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String entityName = c.getPipelineOptions().as(CsvToDatastoreTemplateOptions.class)
                    .getDatastoreEntity().get();
            String text = c.element();
            c.output(toEntity(entityName, text));
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            builder.delegate(ToEntityFn.this);
        }

        @Override
        public TypeDescriptor<String> getInputTypeDescriptor() {
            return TypeDescriptor.of(String.class);
        }

        @Override
        public TypeDescriptor<Entity> getOutputTypeDescriptor() {
            return TypeDescriptor.of(Entity.class);
        }
    }

    private static Entity toEntity(String entityName, String text) {

        String[] split = text.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        return Entity.newBuilder()
                .setKey(Key.newBuilder()
                        .addPath(Key.PathElement.newBuilder()
                                .setName(UUID.randomUUID().toString())
                                .setKind(entityName)
                                .build())
                        .build())
                .putProperties("name", Value.newBuilder()
                        .setStringValue(split[0].trim()).build())
                .putProperties("address", Value.newBuilder()
                        .setStringValue(StringUtils.remove(split[1].trim(), "\"")).build())
                .build();
    }

    interface CsvToDatastoreTemplateOptions extends DataflowPipelineOptions {

        @Description("csv file path")
        @Validation.Required
        ValueProvider<String> getCsvFilePath();
        void setCsvFilePath(ValueProvider<String> csvFilePath);

        @Description("datastore entity name")
        @Validation.Required
        ValueProvider<String> getDatastoreEntity();
        void setDatastoreEntity(ValueProvider<String> datastoreEntity);
    }
}
