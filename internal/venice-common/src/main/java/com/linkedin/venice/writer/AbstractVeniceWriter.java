package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;


/**
 * A base class which users of {@link VeniceWriter} can leverage in order to
 * make unit tests easier.
 *
 * @see VeniceWriter
 * // @see MockVeniceWriter in the VPJ tests (commented because this module does not depend on VPJ)
 */
public abstract class AbstractVeniceWriter<K, V, U> implements Closeable {
  protected final PubSubTopic topicName;
  protected final String topicNameForSerializer;
  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  public AbstractVeniceWriter(PubSubTopic topicName) {
    this.topicName = topicName;
    this.topicNameForSerializer = topicName.getName();
  }

  public PubSubTopic getTopicName() {
    return this.topicName;
  }

  public Future<PubSubProduceResult> put(K key, V value, int valueSchemaId) {
    return put(key, value, valueSchemaId, null);
  }

  public abstract void close(boolean gracefulClose) throws IOException;

  public abstract Future<PubSubProduceResult> put(K key, V value, int valueSchemaId, PubSubProducerCallback callback);

  public abstract Future<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata);

  public abstract Future<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      DeleteMetadata deleteMetadata);

  public abstract Future<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback);

  public abstract void flush();
}
