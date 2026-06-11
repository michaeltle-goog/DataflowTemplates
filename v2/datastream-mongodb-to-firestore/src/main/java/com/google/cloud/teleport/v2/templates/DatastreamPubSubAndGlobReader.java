package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.datastream.sources.ReadFileRangesFn;
import com.google.cloud.teleport.v2.datastream.transforms.FormatDatastreamJsonToJson;
import com.google.cloud.teleport.v2.datastream.transforms.FormatDatastreamRecordToJson;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Watch.Growth;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatastreamPubSubAndGlobReader extends PTransform<PBegin, PCollection<FailsafeElement<String, String>>> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamPubSubAndGlobReader.class);
  private static final String AVRO_SUFFIX = "avro";
  private static final String JSON_SUFFIX = "json";

  private String streamName;
  private String inputFilePattern;
  private String fileType;
  private String gcsNotificationSubscription;
  private String rfcStartDateTime;
  private Integer fileReadConcurrency = 30;
  private Boolean lowercaseSourceColumns = false;
  private Map<String, String> renameColumns = new HashMap<>();
  private Boolean hashRowId = false;
  private Duration directoryWatchDuration = Duration.standardMinutes(10);
  private String datastreamSourceType;

  private Boolean applyReshuffle = true;

  public DatastreamPubSubAndGlobReader(
      String streamName,
      String inputFilePattern,
      String fileType,
      String gcsNotificationSubscription,
      String rfcStartDateTime) {
    this.streamName = streamName;
    this.inputFilePattern = inputFilePattern;
    this.gcsNotificationSubscription = gcsNotificationSubscription;
    this.rfcStartDateTime = rfcStartDateTime;
    this.fileType = fileType;

    if (!(fileType.equals(AVRO_SUFFIX) || fileType.equals(JSON_SUFFIX))) {
      throw new IllegalArgumentException(
          "Input file format must be one of: avro or json - found " + fileType);
    }
  }

  public DatastreamPubSubAndGlobReader withFileReadConcurrency(Integer fileReadConcurrency) {
    this.fileReadConcurrency = fileReadConcurrency;
    return this;
  }

  public DatastreamPubSubAndGlobReader withLowercaseSourceColumns() {
    this.lowercaseSourceColumns = true;
    return this;
  }

  public DatastreamPubSubAndGlobReader withRenameColumnValue(String columnName, String newColumnName) {
    this.renameColumns.put(columnName, newColumnName);
    return this;
  }

  public DatastreamPubSubAndGlobReader withHashRowId() {
    this.hashRowId = true;
    return this;
  }

  public DatastreamPubSubAndGlobReader withDirectoryWatchDuration(Duration directoryWatchDuration) {
    if (directoryWatchDuration != null) {
      this.directoryWatchDuration = directoryWatchDuration;
    }
    return this;
  }

  public DatastreamPubSubAndGlobReader withoutDatastreamRecordsReshuffle() {
    this.applyReshuffle = false;
    return this;
  }

  public DatastreamPubSubAndGlobReader withDatastreamSourceType(String datastreamSourceType) {
    this.datastreamSourceType = datastreamSourceType;
    return this;
  }

  @Override
  public PCollection<FailsafeElement<String, String>> expand(PBegin input) {
    PCollection<ReadableFile> datastreamFiles =
        input.apply("Read Datastream Files", new DataStreamFileIO());
    PCollection<FailsafeElement<String, String>> datastreamJsonStrings =
        expandDataStreamJsonStrings(datastreamFiles);
    return datastreamJsonStrings;
  }

  public PCollection<FailsafeElement<String, String>> expandDataStreamJsonStrings(
      PCollection<ReadableFile> datastreamFiles) {
    PCollection<FailsafeElement<String, String>> datastreamRecords;

    FailsafeElementCoder<String, String> coder =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    if (this.fileType.equals(JSON_SUFFIX)) {
      datastreamRecords =
          datastreamFiles
              .apply(
                  "FileReadConcurrency",
                  Reshuffle.<ReadableFile>viaRandomKey().withNumBuckets(fileReadConcurrency))
              .apply("ReadFiles", TextIO.readFiles())
              .apply("ReshuffleRecords", Reshuffle.viaRandomKey())
              .apply(
                  "ParseJsonRecords",
                  ParDo.of(
                      (FormatDatastreamJsonToJson)
                          FormatDatastreamJsonToJson.create()
                              .withStreamName(this.streamName)
                              .withRenameColumnValues(this.renameColumns)
                              .withHashRowId(this.hashRowId)
                              .withLowercaseSourceColumns(this.lowercaseSourceColumns)
                              .withDatastreamSourceType(this.datastreamSourceType)))
              .setCoder(coder);
    } else {
      SerializableFunction<GenericRecord, FailsafeElement<String, String>> parseFn =
          FormatDatastreamRecordToJson.create()
              .withStreamName(this.streamName)
              .withRenameColumnValues(this.renameColumns)
              .withHashRowId(this.hashRowId)
              .withLowercaseSourceColumns(this.lowercaseSourceColumns)
              .withDatastreamSourceType(this.datastreamSourceType);
      datastreamRecords =
          datastreamFiles
              .apply("ReshuffleFiles", Reshuffle.<ReadableFile>viaRandomKey())
              .apply(
                  "ParseAvroRows",
                  ParDo.of(
                      new ReadFileRangesFn<FailsafeElement<String, String>>(
                          new CreateParseSourceFn(parseFn, coder),
                          new ReadFileRangesFn.ReadFileRangesFnExceptionHandler())))
              .setCoder(coder);
    }
    return applyReshuffle
        ? datastreamRecords.apply("Reshuffle", Reshuffle.viaRandomKey())
        : datastreamRecords;
  }

  private static class CreateParseSourceFn
      implements SerializableFunction<String, FileBasedSource<FailsafeElement<String, String>>> {
    private final SerializableFunction<GenericRecord, FailsafeElement<String, String>> parseFn;
    private final Coder<FailsafeElement<String, String>> coder;

    CreateParseSourceFn(
        SerializableFunction<GenericRecord, FailsafeElement<String, String>> parseFn,
        Coder<FailsafeElement<String, String>> coder) {
      this.parseFn = parseFn;
      this.coder = coder;
    }

    @Override
    public FileBasedSource<FailsafeElement<String, String>> apply(String input) {
      return AvroSource.from(input).withParseFn(parseFn, coder);
    }
  }

  class DataStreamFileIO extends PTransform<PBegin, PCollection<ReadableFile>> {

    @Override
    public PCollection<ReadableFile> expand(PBegin input) {
      PCollection<ReadableFile> datastreamFiles;
      if (!Strings.isNullOrEmpty(gcsNotificationSubscription)) {
        datastreamFiles = expandGcsPubSubPipeline(input);
      } else if (inputFilePattern != null) {
        datastreamFiles = expandPollingPipeline(input);
      } else {
        throw new IllegalArgumentException(
            "DatastreamPubSubAndGlobReader requires either a GCS stream directory or Pub/Sub Subscription");
      }

      return datastreamFiles;
    }

    public PCollection<ReadableFile> expandGcsPubSubPipeline(PBegin input) {
      return input
          .apply(
              "ReadGcsPubSubSubscription",
              PubsubIO.readMessagesWithAttributes().fromSubscription(gcsNotificationSubscription))
          .apply("ExtractGcsFilePath", ParDo.of(new ExtractGcsFile()))
          .apply("ReadFiles", FileIO.readMatches());
    }

    public PCollection<ReadableFile> expandPollingPipeline(PBegin input) {
      return input
          .apply("MatchFiles", FileIO.match()
              .filepattern(inputFilePattern)
              .continuously(
                  directoryWatchDuration,
                  Growth.afterTimeSinceNewOutput(Duration.standardHours(1))))
          .apply("ReadMatches", FileIO.readMatches());
    }
  }

  static class ExtractGcsFile extends DoFn<PubsubMessage, Metadata> {
    @ProcessElement
    public void process(ProcessContext context) throws IOException {
      PubsubMessage message = context.element();

      String eventType = message.getAttribute("eventType");
      String bucketId = message.getAttribute("bucketId");
      String objectId = message.getAttribute("objectId");

      if (eventType != null && eventType.equals("OBJECT_FINALIZE") && objectId != null && !objectId.endsWith("/")) {
        String fileName = "gs://" + bucketId + "/" + objectId;
        try {
          Metadata fileMetadata = FileSystems.matchSingleFileSpec(fileName);
          context.output(fileMetadata);
        } catch (FileNotFoundException e) {
          LOG.warn("Ignoring non-existent file {}", fileName, e);
        } catch (IOException e) {
          LOG.error("GCS Failure retrieving {}", fileName, e);
          throw e;
        }
      }
    }
  }
}
