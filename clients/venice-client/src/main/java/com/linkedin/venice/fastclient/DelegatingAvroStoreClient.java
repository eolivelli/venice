package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;


/**
 * Inside Fast-Client, we choose to use n-tier architecture style to build a pipeline to separate different
 * types of logic in different layer.
 *
 * n-tier architecture => Having multiple layers where each layer wraps the next inner one.
 * Each layer provides some functionality, e.g. stats collection, etc.
 *
 * Fast-Client's layers include the below components. Check {@link ClientFactory#getAndStartGenericStoreClient}
 * to figure out how the layers are put together for different requirements.
 *
 * Layer -1:
 * AvroGenericStoreClient => interface: Borrowed from thin-client
 *
 * Layer 0:
 * InternalAvroStoreClient implements AvroGenericStoreClient => Only 1 abstract class implementing above interface:
 *                            Both thin-client and fast-client uses this as the Layer 0. All the internal
 *                            implementations of different tiers should extend this class.
 *
 * Layer 1:
 * DispatchingAvroGenericStoreClient extends InternalAvroStoreClient => in charge of routing and serialization/de-serialization
 *
 * Layer 2:
 * RetriableAvroGenericStoreClient extends DelegatingAvroStoreClient => Adds optional retry ability on top of DispatchingAvroGenericStoreClient
 *
 * Layer 3:
 * StatsAvroGenericStoreClient extends DelegatingAvroStoreClient => Adds stats on top of Layer 2 or Layer 1. There is no option
 *                            to disable it, but if needed, can be disabled.
 *
 * Layer 4:
 * DualReadAvroGenericStoreClient extends DelegatingAvroStoreClient => Adds an extra read via thin client on top of Layer 3.
 *
 * utils class:
 * DelegatingAvroStoreClient extends InternalAvroStoreClient => Delegator pattern to not override all the
 *                           functions in every superclass in a duplicate manner.
 *
 * Interactions between these class for some flows: https://swimlanes.io/u/D3E9Q50pb
 */

public class DelegatingAvroStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  private final InternalAvroStoreClient<K, V> delegate;

  public DelegatingAvroStoreClient(InternalAvroStoreClient<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    return delegate.get(requestContext, key);
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    return delegate.batchGet(keys);
  }

  @Override
  // Future implementation after stabilization of streaming batch get
  /**
   * This implementation is for future use. It will get wired in via
   * InternalAvroStoreClient.batchGet(Set<K> keys)
   */
  protected CompletableFuture<Map<K, V>> batchGet(BatchGetRequestContext<K, V> requestContext, Set<K> keys)
      throws VeniceClientException {
    return delegate.batchGet(requestContext, keys);
  }

  @Override
  protected void streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys,
      StreamingCallback<K, V> callback) {
    delegate.streamingBatchGet(requestContext, keys, callback);
  }

  @Override
  protected CompletableFuture<VeniceResponseMap<K, V>> streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys) {
    return delegate.streamingBatchGet(requestContext, keys);
  }

  @Override
  public void start() throws VeniceClientException {
    delegate.start();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public String getStoreName() {
    return delegate.getStoreName();
  }

  @Override
  public Schema getKeySchema() {
    return delegate.getKeySchema();
  }

  @Override
  public Schema getLatestValueSchema() {
    return delegate.getLatestValueSchema();
  }
}
