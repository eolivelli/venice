package com.linkedin.davinci.repository;

import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StoreStateReader;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SystemStore;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This implementation uses DaVinci client backed meta system store to provide data to the {@link NativeMetadataRepository}.
 * The data is then cached and served from there.
 */
public class DaVinciClientMetaStoreBasedRepository extends NativeMetadataRepository {
  private static final int KEY_SCHEMA_ID = 1;
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientMetaStoreBasedRepository.class);

  // Map of user store name to their corresponding meta store which is used for finding the correct current version.
  // TODO Store objects in this map are mocked locally based on client.meta.system.store.version.map config.

  // A map of user store name to their corresponding daVinci client of the meta store.
  private final Map<String, DaVinciClient<StoreMetaKey, StoreMetaValue>> daVinciClientMap =
      new VeniceConcurrentHashMap<>();

  // A map of mocked meta Store objects. Keep the meta stores separately from
  // SystemStoreBasedRepository.subscribedStoreMap
  // because these meta stores doesn't require refreshes.
  private final Map<String, SystemStore> metaStoreMap = new VeniceConcurrentHashMap<>();

  private final DaVinciConfig daVinciConfig = new DaVinciConfig();
  private final CachingDaVinciClientFactory daVinciClientFactory;
  private final SchemaReader metaStoreSchemaReader;

  private final StoreDataChangedListener metaSystemStoreChangeListener = new StoreDataChangedListener() {
    @Override
    public void handleStoreChanged(Store store) {
      if (!(store instanceof SystemStore)) {
        String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(store.getName());
        SystemStore existingMetaSystemStore = metaStoreMap.get(metaSystemStoreName);
        if (existingMetaSystemStore == null) {
          LOGGER.warn("Meta system store: {} is missing unexpectedly from internal metaStoreMap", metaSystemStoreName);
          return;
        }
        // Even if there's a rewind and we accidentally go back to a previous version it should be temporary.
        // i.e. endpoint discovers v2 -> meta system store attributes shows v1 (due to rewind) -> endpoint will discover
        // v2 again
        SystemStore newMetaSystemStore = new SystemStore(
            existingMetaSystemStore.getZkSharedStore().cloneStore(),
            existingMetaSystemStore.getSystemStoreType(),
            store.cloneStore());
        metaStoreMap.put(metaSystemStoreName, newMetaSystemStore);
        notifyStoreChanged(newMetaSystemStore);
      }
    }
  };

  public DaVinciClientMetaStoreBasedRepository(ClientConfig clientConfig, VeniceProperties backendConfig) {
    super(clientConfig, backendConfig);
    daVinciClientFactory = new CachingDaVinciClientFactory(
        clientConfig.getD2Client(),
        clientConfig.getD2ServiceName(),
        Optional.ofNullable(clientConfig.getMetricsRepository())
            .orElse(TehutiUtils.getMetricsRepository("davinci-client")),
        backendConfig);
    ClientConfig clonedClientConfig = ClientConfig.cloneConfig(clientConfig)
        .setStoreName(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName())
        .setSpecificValueClass(StoreMetaValue.class);
    metaStoreSchemaReader = ClientFactory.getSchemaReader(clonedClientConfig, null);
    registerStoreDataChangedListener(metaSystemStoreChangeListener);
  }

  @Override
  protected Store removeStore(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == null) {
      // We also need to release its corresponding meta system store resources.
      Store metaSystemStore = metaStoreMap.remove(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName));
      if (metaSystemStore != null) {
        notifyStoreDeleted(metaSystemStore);
      }
      daVinciClientMap.remove(storeName);
    }
    return super.removeStore(storeName);
  }

  @Override
  protected StoreMetaValue getStoreMetaValue(String storeName, StoreMetaKey key) {
    StoreMetaValue value;
    try {
      value = getDaVinciClientForMetaStore(storeName).get(key).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceException(
          "Failed to get metadata from meta system store with DaVinci client for store: " + storeName + " with key: "
              + key.toString(),
          e);
    }
    if (value == null) {
      throw new MissingKeyInStoreMetadataException(key.toString(), StoreMetaValue.class.getSimpleName());
    }
    return value;
  }

  @Override
  public SchemaEntry getKeySchema(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      return new SchemaEntry(KEY_SCHEMA_ID, metaStoreSchemaReader.getKeySchema());
    } else {
      return super.getKeySchema(storeName);
    }
  }

  @Override
  protected SchemaEntry getValueSchemaInternally(String storeName, int id) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      Schema schema = metaStoreSchemaReader.getValueSchema(id);
      return schema == null ? null : new SchemaEntry(id, schema);
    } else {
      return super.getValueSchemaInternally(storeName, id);
    }
  }

  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      return metaStoreSchemaReader.getValueSchemaId(Schema.parse(valueSchemaStr));
    } else {
      return super.getValueSchemaId(storeName, valueSchemaStr);
    }
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      throw new UnsupportedOperationException("getValueSchemas not supported for store: " + storeName);
    } else {
      return super.getValueSchemas(storeName);
    }
  }

  @Override
  public SchemaEntry getSupersetOrLatestValueSchema(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      return new SchemaEntry(
          metaStoreSchemaReader.getLatestValueSchemaId(),
          metaStoreSchemaReader.getLatestValueSchema());
    } else {
      return super.getSupersetOrLatestValueSchema(storeName);
    }
  }

  @Override
  public Optional<SchemaEntry> getSupersetSchema(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      throw new VeniceException("Meta store does not have superset schema. Store name: " + storeName);
    } else {
      return super.getSupersetSchema(storeName);
    }
  }

  @Override
  public void subscribe(String storeName) throws InterruptedException {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      metaStoreMap.computeIfAbsent(storeName, k -> getMetaStore(storeName));
    } else {
      super.subscribe(storeName);
    }
  }

  @Override
  public Store getStore(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      Store store = metaStoreMap.get(storeName);
      return store == null ? null : new ReadOnlyStore(store);
    } else {
      return super.getStore(storeName);
    }
  }

  @Override
  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      Store store = metaStoreMap.get(storeName);
      if (store != null) {
        return new ReadOnlyStore(store);
      }
      throw new VeniceNoStoreException(storeName);
    } else {
      return super.getStoreOrThrow(storeName);
    }
  }

  @Override
  public boolean hasStore(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      return metaStoreMap.containsKey(storeName);
    } else {
      return super.hasStore(storeName);
    }
  }

  @Override
  public void clear() {
    subscribedStoreMap.clear();
    daVinciClientFactory.close();
    daVinciClientMap.clear();
    Utils.closeQuietlyWithErrorLogged(metaStoreSchemaReader);
    subscribedStoreMap.clear();
  }

  @Override
  protected StoreConfig getStoreConfigFromSystemStore(String storeName) {
    return getStoreConfigFromMetaSystemStore(storeName);
  }

  @Override
  protected Store getStoreFromSystemStore(String storeName, String clusterName) {
    StoreProperties storeProperties =
        getStoreMetaValue(storeName, MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
            put(KEY_STRING_CLUSTER_NAME, clusterName);
          }
        })).storeProperties;
    return new ZKStore(storeProperties);
  }

  private SystemStore getMetaStore(String metaStoreName) {
    ClientConfig clonedClientConfig = ClientConfig.cloneConfig(clientConfig).setStoreName(metaStoreName);
    try (StoreStateReader storeStateReader = StoreStateReader.getInstance(clonedClientConfig)) {
      Store metaStore = storeStateReader.getStore();
      if (metaStore instanceof SystemStore) {
        return (SystemStore) metaStore;
      } else {
        throw new VeniceException(
            "Expecting a meta system store from StoreStateReader with name: " + metaStoreName
                + " but got a non-system store instead");
      }
    }
  }

  @Override
  protected SchemaData getSchemaDataFromSystemStore(String storeName) {
    return getSchemaDataFromMetaSystemStore(storeName);
  }

  private DaVinciClient<StoreMetaKey, StoreMetaValue> getDaVinciClientForMetaStore(String storeName) {
    return daVinciClientMap.computeIfAbsent(storeName, k -> {
      long metaStoreDVCStartTime = System.currentTimeMillis();
      DaVinciClient<StoreMetaKey, StoreMetaValue> client = daVinciClientFactory.getAndStartSpecificAvroClient(
          VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName),
          daVinciConfig,
          StoreMetaValue.class);
      try {
        client.subscribeAll().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new VeniceException("Failed to construct DaVinci client for the meta store of store: " + storeName, e);
      }
      LOGGER.info(
          "DaVinci client for the meta store of store: {} constructed, took: {} ms",
          storeName,
          System.currentTimeMillis() - metaStoreDVCStartTime);
      return client;
    });
  }
}
