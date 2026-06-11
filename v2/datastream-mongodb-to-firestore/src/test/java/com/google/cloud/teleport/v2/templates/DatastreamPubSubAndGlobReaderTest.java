package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatastreamPubSubAndGlobReaderTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testFileDiscoveryWithGlobsJson() throws IOException {
    // Create nested directories without any "0-byte markers"
    File subdir = tempFolder.newFolder("foo", "bar");
    File testFile = new File(subdir, "test-event.json");
    
    // Write a dummy JSON event that resembles Datastream output
    String jsonEvent = "{\"_metadata_table\":\"test_table\",\"_metadata_source\":\"mongodb\",\"data\":{\"_id\":\"123\"}}";
    Files.write(testFile.toPath(), Collections.singletonList(jsonEvent));

    // The glob should be able to find the file correctly
    String globPattern = tempFolder.getRoot().getAbsolutePath() + "/**/*.json";

    DatastreamPubSubAndGlobReader reader =
        new DatastreamPubSubAndGlobReader(
            "testStream", globPattern, "json", null, null)
            .withFileReadConcurrency(1)
            .withoutDatastreamRecordsReshuffle();

    PCollection<FailsafeElement<String, String>> elements = pipeline.apply(reader);

    PAssert.that(elements).satisfies(iterable -> {
      int count = 0;
      for (FailsafeElement<String, String> element : iterable) {
        count++;
        assertNotNull(element.getPayload());
      }
      assertEquals(1, count);
      return null;
    });

    pipeline.run();
  }

  @Test
  public void testFileDiscoveryWithGlobsAvro() throws IOException {
    File subdir = tempFolder.newFolder("avro_foo", "bar");
    File testFile = new File(subdir, "test-event.avro");

    String schemaString = "{\"type\": \"record\", \"name\": \"Event\", \"fields\": ["
        + "{\"name\": \"_metadata_table\", \"type\": \"string\"},"
        + "{\"name\": \"_metadata_source\", \"type\": \"string\"},"
        + "{\"name\": \"data\", \"type\": \"string\"}"
        + "]}";
    Schema schema = new Schema.Parser().parse(schemaString);

    GenericRecord record = new GenericData.Record(schema);
    record.put("_metadata_table", "test_table");
    record.put("_metadata_source", "mongodb");
    record.put("data", "{\"_id\":\"123\"}");

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(schema, testFile);
      dataFileWriter.append(record);
    }

    String globPattern = tempFolder.getRoot().getAbsolutePath() + "/**/*.avro";

    DatastreamPubSubAndGlobReader reader =
        new DatastreamPubSubAndGlobReader(
            "testStream", globPattern, "avro", null, null)
            .withFileReadConcurrency(1)
            .withoutDatastreamRecordsReshuffle();

    PCollection<FailsafeElement<String, String>> elements = pipeline.apply("ReadAvro", reader);

    PAssert.that(elements).satisfies(iterable -> {
      int count = 0;
      for (FailsafeElement<String, String> element : iterable) {
        count++;
        assertNotNull(element.getPayload());
      }
      assertEquals(1, count);
      return null;
    });

    pipeline.run();
  }
}
