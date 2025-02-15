package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.LF_MODEL_DEPENDENCY_CHECK_DISABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_KAFKA_PRODUCER_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.INCREMENTAL_PUSH;
import static com.linkedin.venice.hadoop.VenicePushJob.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestActiveActiveReplicationForIncPush {
  private static final Logger LOGGER = LogManager.getLogger(TestActiveActiveReplicationForIncPush.class);

  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;

  private static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  KafkaBrokerWrapper veniceParentDefaultKafka;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 3 seconds;
     * Create a testing environment with 1 parent fabric and 3 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     */
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, "true");
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "1");

    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1000);
    controllerProps.put(LF_MODEL_DEPENDENCY_CHECK_DISABLED, "true");
    controllerProps.put(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, "true");

    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        2,
        1,
        2,
        Optional.of(new VeniceProperties(controllerProps)),
        Optional.of(controllerProps),
        Optional.of(new VeniceProperties(serverProperties)),
        false);
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();

    veniceParentDefaultKafka = multiColoMultiClusterWrapper.getParentKafkaBrokerWrapper();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  /**
   * The purpose of this test is to verify that incremental push with RT policy succeeds when A/A is enabled in all colos.
   * And also incremental push can push to the closes kafka cluster from the grid using the SOURCE_GRID_CONFIG.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testAAReplicationForIncrementalPushToRT() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDirBatch = getTempDataDirectory();
    File inputDirInc1 = getTempDataDirectory();
    File inputDirInc2 = getTempDataDirectory();

    String parentControllerUrls =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    String inputDirPathBatch = "file:" + inputDirBatch.getAbsolutePath();
    String inputDirPathInc1 = "file:" + inputDirInc1.getAbsolutePath();
    String inputDirPathInc2 = "file:" + inputDirInc2.getAbsolutePath();
    Function<Integer, String> connectionString = i -> childDatacenters.get(i).getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrls);
        ControllerClient dc0ControllerClient = new ControllerClient(clusterName, connectionString.apply(0));
        ControllerClient dc1ControllerClient = new ControllerClient(clusterName, connectionString.apply(1));
        ControllerClient dc2ControllerClient = new ControllerClient(clusterName, connectionString.apply(2))) {
      String storeName = Utils.getUniqueString("store");
      Properties propsBatch = TestWriteUtils.defaultVPJProps(parentControllerUrls, inputDirPathBatch, storeName);
      propsBatch.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
      Properties propsInc1 = TestWriteUtils.defaultVPJProps(parentControllerUrls, inputDirPathInc1, storeName);
      propsInc1.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
      Properties propsInc2 = TestWriteUtils.defaultVPJProps(parentControllerUrls, inputDirPathInc2, storeName);
      propsInc2.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithUserSchema(inputDirBatch, true, 100);
      String keySchemaStr = recordSchema.getField(VenicePushJob.DEFAULT_KEY_FIELD_PROP).schema().toString();
      String valueSchemaStr = recordSchema.getField(VenicePushJob.DEFAULT_VALUE_FIELD_PROP).schema().toString();

      propsInc1.setProperty(INCREMENTAL_PUSH, "true");
      propsInc1.put(SOURCE_GRID_FABRIC, "dc-2");
      TestWriteUtils.writeSimpleAvroFileWithUserSchema2(inputDirInc1);

      propsInc2.setProperty(INCREMENTAL_PUSH, "true");
      propsInc2.put(SOURCE_GRID_FABRIC, "dc-1");
      TestWriteUtils.writeSimpleAvroFileWithUserSchema3(inputDirInc2);

      TestUtils.assertCommand(parentControllerClient.createNewStore(storeName, "owner", keySchemaStr, valueSchemaStr));
      // Store Setup
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setPartitionCount(1)
              .setHybridOffsetLagThreshold(TEST_TIMEOUT / 2)
              .setHybridRewindSeconds(2L)
              .setIncrementalPushEnabled(true)
              .setLeaderFollowerModel(true)
              .setNativeReplicationEnabled(true)
              .setNativeReplicationSourceFabric("dc-2");
      TestUtils.assertCommand(parentControllerClient.updateStore(storeName, updateStoreParams));

      UpdateStoreQueryParams enableAARepl = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true);

      // Print all the kafka cluster URLs
      LOGGER.info("KafkaURL dc-0:{}", childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
      LOGGER.info("KafkaURL dc-1:{}", childDatacenters.get(1).getKafkaBrokerWrapper().getAddress());
      LOGGER.info("KafkaURL dc-2:{}", childDatacenters.get(2).getKafkaBrokerWrapper().getAddress());
      LOGGER.info("KafkaURL dc-parent-0:{}", veniceParentDefaultKafka.getAddress());

      // Turn on A/A in parent to trigger auto replication metadata schema registration
      TestWriteUtils.updateStore(storeName, parentControllerClient, enableAARepl);

      // verify store configs
      TestUtils.verifyDCConfigNativeAndActiveRepl(parentControllerClient, storeName, true, true);
      TestUtils.verifyDCConfigNativeAndActiveRepl(dc0ControllerClient, storeName, true, true);
      TestUtils.verifyDCConfigNativeAndActiveRepl(dc1ControllerClient, storeName, true, true);
      TestUtils.verifyDCConfigNativeAndActiveRepl(dc2ControllerClient, storeName, true, true);

      // Run a batch push first
      try (VenicePushJob job = new VenicePushJob("Test push job batch with NR + A/A all fabrics", propsBatch)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(2).getKafkaBrokerWrapper().getAddress());
      }
      // Run inc push with source fabric preference taking effect.
      try (VenicePushJob job = new VenicePushJob("Test push job incremental with NR + A/A from dc-2", propsInc1)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(2).getKafkaBrokerWrapper().getAddress());
      }

      // Verify
      for (int i = 0; i < childDatacenters.size(); i++) {
        VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(i);
        // Verify the current version should be 1.
        Optional<Version> version =
            childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(1);
        Assert.assertTrue(version.isPresent(), "Version 1 is not present for DC: " + i);
      }
      NativeReplicationTestUtils.verifyIncrementalPushData(childDatacenters, clusterName, storeName, 150, 2);

      // Run another inc push with a different source fabric preference taking effect.
      try (VenicePushJob job = new VenicePushJob("Test push job incremental with NR + A/A from dc-1", propsInc2)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(1).getKafkaBrokerWrapper().getAddress());
      }
      NativeReplicationTestUtils.verifyIncrementalPushData(childDatacenters, clusterName, storeName, 200, 3);
    }
  }
}
