package dataflow.examples.basic;

import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;

public class MigratingToBigQueryTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void shouldTransformToTableRowV1() {

        PCollection<TableRow> tableRows = pipeline
                .apply("Seed data", Create.of(
                        entity("FirstName1 LastName1", "Address1"),
                        entity("FirstName2 LastName2", "Address2"),
                        entity("FirstName3 LastName3", "Address3")
                ))
                .apply("To TableRow", MapElements
                        .into(TypeDescriptor.of(TableRow.class))
                        .via(MigratingToBigQuery::toTableRow));

        PAssert.that(tableRows).containsInAnyOrder(
                tableRow("FirstName1 LastName1", "Address1"),
                tableRow("FirstName2 LastName2", "Address2"),
                tableRow("FirstName3 LastName3", "Address3")
        );

        pipeline.run();
    }

    @Test
    public void shouldTransformToTableRowV2() {

        PCollection<TableRow> tableRows = pipeline
                .apply("Seed data", Create.of(
                        entity("FirstName1 LastName1", "35 School Ln, Roxton1, UK"),
                        entity("FirstName2 LastName2", "35 School Ln, Roxton2, UK"),
                        entity("FirstName3 LastName3", "35 School Ln, Roxton3, UK")
                ))
                .apply("To TableRow", MapElements
                        .into(TypeDescriptor.of(TableRow.class))
                        .via(MigratingToBigQuery::toTableRowV2));

        PAssert.that(tableRows).containsInAnyOrder(
                tableRowV2("FirstName1", "LastName1", "35 School Ln, Roxton1, UK", "Roxton1"),
                tableRowV2("FirstName2", "LastName2", "35 School Ln, Roxton2, UK", "Roxton2"),
                tableRowV2("FirstName3", "LastName3", "35 School Ln, Roxton3, UK", "Roxton3")
        );

        pipeline.run();
    }

    private Entity entity(String name, String address) {
        return Entity.newBuilder()
                .putProperties("name", Value.newBuilder().setStringValue(name).build())
                .putProperties("address", Value.newBuilder().setStringValue(address).build())
                .build();
    }

    private TableRow tableRow(String name, String address) {
        return new TableRow()
                .set("name", name)
                .set("address", address);
    }

    private TableRow tableRowV2(String firstName, String lastName, String address, String suburb) {
        return new TableRow()
                .set("firstName", firstName)
                .set("lastName", lastName)
                .set("address", address)
                .set("suburb", suburb);
    }

}
