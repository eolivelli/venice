package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import com.linkedin.venice.store.bdb.BdbStorageEngineFactory;
import com.linkedin.venice.store.memory.InMemoryStorageEngineFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import org.apache.log4j.Logger;

import java.util.Map;

import static com.linkedin.venice.meta.PersistenceType.*;


/**
 * Storage interface to Venice Server. Manages creation and deletion of of Storage engines
 * and Partitions.
 *
 * Use StoreRepository, if read only access is desired for the Storage Engines.
 */
public class StorageService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(StorageService.class);

  private final StoreRepository storeRepository;
  private final VeniceServerConfig serverConfig;

  private final Map<PersistenceType, StorageEngineFactory> persistenceTypeToStorageEngineFactoryMap;
  private final PartitionAssignmentRepository partitionAssignmentRepository;

  public StorageService(VeniceConfigLoader configLoader) {
    this.serverConfig = configLoader.getVeniceServerConfig();
    this.storeRepository = new StoreRepository();
    this.persistenceTypeToStorageEngineFactoryMap = new HashMap<>();
    this.partitionAssignmentRepository = new PartitionAssignmentRepository();
    initInternalStorageEngineFactories();
    // Restore all the stores persisted previously
    restoreAllStores(configLoader);
  }

  /**
   * Initialize all the internal storage engine factories.
   * Please add it here if you want to add more.
   */
  private void initInternalStorageEngineFactories() {
    persistenceTypeToStorageEngineFactoryMap.put(BDB, new BdbStorageEngineFactory(serverConfig));
    persistenceTypeToStorageEngineFactoryMap.put(IN_MEMORY, new InMemoryStorageEngineFactory(serverConfig));
  }

  private void restoreAllStores(VeniceConfigLoader configLoader) {
    logger.info("Start restoring all the stores persisted previously");
    for (Map.Entry<PersistenceType, StorageEngineFactory> entry : persistenceTypeToStorageEngineFactoryMap.entrySet()) {
      PersistenceType pType = entry.getKey();
      StorageEngineFactory factory = entry.getValue();
      logger.info("Start restoring all the stores with type: " + pType);
      Set<String> storeNames = factory.getPersistedStoreNames();
      for (String storeName : storeNames) {
        logger.info("Start restoring store: " + storeName + " with type: " + pType);
        /**
         * The logic is a little weird, since right now Venice doesn't have store-level persistence config.
         * TODO: In case we want to specify different persistence type per store, this part might need to be changed.
          */
        VeniceStoreConfig storeConfig = configLoader.getStoreConfig(storeName);
        AbstractStorageEngine storageEngine = openStore(storeConfig);
        Set<Integer> partitionIds = storageEngine.getPartitionIds();
        for (Integer partitionId : partitionIds) {
          partitionAssignmentRepository.addPartition(storeName, partitionId);
        }
        logger.info("Loaded the following partitions: " + Arrays.toString(partitionIds.toArray()) + ", for store: " + storeName);
        logger.info("Done restoring store: " + storeName + " with type: " + pType);
      }
      logger.info("Done restoring all the stores with type: " + pType);
    }
    logger.info("Done restoring all the stores persisted previously");
  }

  // TODO Later change to Guice instead of Java reflections
  // This method can also be called from an admin service to add new store.

  public AbstractStorageEngine openStoreForNewPartition(VeniceStoreConfig storeConfig, int partitionId) {
    partitionAssignmentRepository.addPartition(storeConfig.getStoreName(), partitionId);

    AbstractStorageEngine engine = openStore(storeConfig);
    if (!engine.containsPartition(partitionId)) {
      engine.addStoragePartition(partitionId);
    }
    return engine;
  }

  /**
   * This method should ideally be Private, but marked as public for validating the result.
   *
   * @param storeConfig StoreConfig of the store.
   * @return Factory corresponding to the store.
   */
  public StorageEngineFactory getInternalStorageEngineFactory(VeniceStoreConfig storeConfig) {
    PersistenceType persistenceType = storeConfig.getPersistenceType();
    // Instantiate the factory for this persistence type if not already present
    if (persistenceTypeToStorageEngineFactoryMap.containsKey(persistenceType)) {
      return persistenceTypeToStorageEngineFactoryMap.get(persistenceType);
    }

    throw new VeniceException("Unrecognized persistence type " + persistenceType);
  }


  /**
   * Creates a StorageEngineFactory for the persistence type if not already present.
   * Creates a new storage engine for the given store in the factory and registers the storage engine with the store repository.
   *
   * @param storeConfig   The store specific properties
   * @return StorageEngine that was created for the given store definition.
   */
  private synchronized AbstractStorageEngine openStore(VeniceStoreConfig storeConfig) {
    String storeName = storeConfig.getStoreName();
    AbstractStorageEngine engine = storeRepository.getLocalStorageEngine(storeName);
    if (engine != null) {
      return engine;
    }

    logger.info("Creating/Opening Storage Engine " + storeName);
    StorageEngineFactory factory = getInternalStorageEngineFactory(storeConfig);
    engine = factory.getStore(storeConfig);
    storeRepository.addLocalStorageEngine(engine);
    return engine;
  }

  /**
   * Removes the Store, Partition from the Storage service.
   */
  public synchronized void dropStorePartition(VeniceStoreConfig storeConfig, int partitionId) {
    String storeName = storeConfig.getStoreName();

    partitionAssignmentRepository.dropPartition(storeName , partitionId);
    AbstractStorageEngine storageEngine = storeRepository.getLocalStorageEngine(storeName);
    if (storageEngine == null) {
      logger.info(storeName + " Store could not be located, ignoring the remove partition message.");
      return;
    }

    storageEngine.dropPartition(partitionId);
    Set<Integer> assignedPartitions = storageEngine.getPartitionIds();

    logger.info("Dropped Partition " + partitionId + " Store " + storeName + " Remaining " + Arrays.toString(assignedPartitions.toArray()));
    if (assignedPartitions.isEmpty()) {
      logger.info("All partitions for Store " + storeName + " are removed... cleaning up state");

      // Drop the directory completely.
      StorageEngineFactory factory = getInternalStorageEngineFactory(storeConfig);
      factory.removeStorageEngine(storageEngine);

      // Clean up the state
      storeRepository.removeLocalStorageEngine(storeName);
    }
  }

  public StoreRepository getStoreRepository() {
    return storeRepository;
  }

  @Override
  public boolean startInner() throws Exception {
    // After Storage Node starts, Helix controller initiates the state transition for the Stores that
    // should be consumed/served by the router.

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner()
      throws VeniceException {
    VeniceException lastException = null;
    try {
      this.storeRepository.close();
    } catch (VeniceException e) {
      lastException = e;
    }

    /*Close all storage engine factories */
    for (Map.Entry<PersistenceType, StorageEngineFactory> storageEngineFactory : persistenceTypeToStorageEngineFactoryMap.entrySet()) {
      PersistenceType factoryType =  storageEngineFactory.getKey();
      logger.info("Closing " + factoryType + " storage engine factory");
      try {
        storageEngineFactory.getValue().close();
      } catch (VeniceException e) {
        logger.error("Error closing " + factoryType, e);
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    }
  }
}
