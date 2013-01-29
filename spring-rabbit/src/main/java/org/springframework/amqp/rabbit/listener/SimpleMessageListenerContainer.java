/*
 * Copyright 2002-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.listener;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.aopalliance.aop.Advice;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.ShouldRetryLocallyException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.retry.RetryStrategy;
import org.springframework.amqp.rabbit.retry.TemplateRetryStrategy;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.aop.Pointcut;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.support.MetricType;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @since 1.0
 */
public class SimpleMessageListenerContainer extends
		AbstractMessageListenerContainer {

	public static final long DEFAULT_RECEIVE_TIMEOUT = 1000;

	public static final int DEFAULT_PREFETCH_COUNT = 1;

	public static final long DEFAULT_SHUTDOWN_TIMEOUT = 5000;

	public static final int DEFAULT_REQUE_TIMES = 2;

	/**
	 * The default recovery interval: 5000 ms = 5 seconds.
	 */
	public static final long DEFAULT_RECOVERY_INTERVAL = 5000;

	private volatile int prefetchCount = DEFAULT_PREFETCH_COUNT;

	private volatile int txSize = 1;

	private volatile Executor taskExecutor = new SimpleAsyncTaskExecutor();

	private volatile int concurrentConsumers = 1;

	private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

	private long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

	private long recoveryInterval = DEFAULT_RECOVERY_INTERVAL;

	private int requeTimes = DEFAULT_REQUE_TIMES;

	private Set<BlockingQueueConsumer> consumers;

	private final Object consumersMonitor = new Object();

	private PlatformTransactionManager transactionManager;

	private TransactionAttribute transactionAttribute = new DefaultTransactionAttribute();

	private volatile Advice[] adviceChain = new Advice[0];

	private ActiveObjectCounter<BlockingQueueConsumer> cancellationLock = new ActiveObjectCounter<BlockingQueueConsumer>();

	private volatile MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private volatile boolean defaultRequeueRejected = true;

	private RetryStrategy retryStrategy = new TemplateRetryStrategy(0,
			requeTimes);

	public static interface ContainerDelegate {
		void invokeListener(Channel channel, Message message) throws Exception;
	}

	private ContainerDelegate delegate = new ContainerDelegate() {
		public void invokeListener(Channel channel, Message message)
				throws Exception {
			SimpleMessageListenerContainer.super.invokeListener(channel,
					message);
		}
	};

	private ContainerDelegate proxy = delegate;

	/**
	 * Default constructor for convenient dependency injection via setters.
	 */
	public SimpleMessageListenerContainer() {
	}

	/**
	 * Create a listener container from the connection factory (mandatory).
	 * 
	 * @param connectionFactory
	 *            the {@link ConnectionFactory}
	 */
	public SimpleMessageListenerContainer(ConnectionFactory connectionFactory) {
		this.setConnectionFactory(connectionFactory);
	}

	/**
	 * <p>
	 * Public setter for the {@link Advice} to apply to listener executions. If
	 * {@link #setTxSize(int) txSize>1} then multiple listener executions will
	 * all be wrapped in the same advice up to that limit.
	 * </p>
	 * <p>
	 * If a {@link #setTransactionManager(PlatformTransactionManager)
	 * transactionManager} is provided as well, then separate advice is created
	 * for the transaction and applied first in the chain. In that case the
	 * advice chain provided here should not contain a transaction interceptor
	 * (otherwise two transactions would be be applied).
	 * </p>
	 * 
	 * @param adviceChain
	 *            the advice chain to set
	 */
	public void setAdviceChain(Advice[] adviceChain) {
		this.adviceChain = adviceChain;
	}

	/**
	 * Specify the interval between recovery attempts, in <b>milliseconds</b>.
	 * The default is 5000 ms, that is, 5 seconds.
	 */
	public void setRecoveryInterval(long recoveryInterval) {
		this.recoveryInterval = recoveryInterval;
	}

	/**
	 * Specify the number of concurrent consumers to create. Default is 1.
	 * <p>
	 * Raising the number of concurrent consumers is recommended in order to
	 * scale the consumption of messages coming in from a queue. However, note
	 * that any ordering guarantees are lost once multiple consumers are
	 * registered. In general, stick with 1 consumer for low-volume queues.
	 */
	public void setConcurrentConsumers(int concurrentConsumers) {
		Assert.isTrue(concurrentConsumers > 0,
				"'concurrentConsumers' value must be at least 1 (one)");
		this.concurrentConsumers = concurrentConsumers;
	}

	public void setReceiveTimeout(long receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	/**
	 * The time to wait for workers in milliseconds after the container is
	 * stopped, and before the connection is forced closed. If any workers are
	 * active when the shutdown signal comes they will be allowed to finish
	 * processing as long as they can finish within this timeout. Otherwise the
	 * connection is closed and messages remain unacked (if the channel is
	 * transactional). Defaults to 5 seconds.
	 * 
	 * @param shutdownTimeout
	 *            the shutdown timeout to set
	 */
	public void setShutdownTimeout(long shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}

	public void setTaskExecutor(Executor taskExecutor) {
		Assert.notNull(taskExecutor, "taskExecutor must not be null");
		this.taskExecutor = taskExecutor;
	}

	/**
	 * Tells the broker how many messages to send to each consumer in a single
	 * request. Often this can be set quite high to improve throughput. It
	 * should be greater than or equal to {@link #setTxSize(int) the transaction
	 * size}.
	 * 
	 * @param prefetchCount
	 *            the prefetch count
	 */
	public void setPrefetchCount(int prefetchCount) {
		this.prefetchCount = prefetchCount;
	}

	/**
	 * Tells the container how many messages to process in a single transaction
	 * (if the channel is transactional). For best results it should be less
	 * than or equal to {@link #setPrefetchCount(int) the prefetch count}.
	 * 
	 * @param txSize
	 *            the transaction size
	 */
	public void setTxSize(int txSize) {
		this.txSize = txSize;
	}

	public void setTransactionManager(
			PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	/**
	 * @param transactionAttribute
	 *            the transaction attribute to set
	 */
	public void setTransactionAttribute(
			TransactionAttribute transactionAttribute) {
		this.transactionAttribute = transactionAttribute;
	}

	/**
	 * Set the {@link MessagePropertiesConverter} for this listener container.
	 */
	public void setMessagePropertiesConverter(
			MessagePropertiesConverter messagePropertiesConverter) {
		Assert.notNull(messagePropertiesConverter,
				"messagePropertiesConverter must not be null");
		this.messagePropertiesConverter = messagePropertiesConverter;
	}

	/**
	 * Determines the default behavior when a message is rejected, for example
	 * because the listener threw an exception. When true, messages will be
	 * requeued, when false, they will not. For versions of Rabbit that support
	 * dead-lettering, the message must not be requeued in order to be sent to
	 * the dead letter exchange. Setting to false causes all rejections to not
	 * be requeued. When true, the default can be overridden by the listener
	 * throwing an {@link AmqpRejectAndDontRequeueException}. Default true.
	 * 
	 * @param defaultRequeueRejected
	 */
	public void setDefaultRequeueRejected(boolean defaultRequeueRejected) {
		this.defaultRequeueRejected = defaultRequeueRejected;
	}

	/**
	 * Avoid the possibility of not configuring the CachingConnectionFactory in
	 * sync with the number of concurrent consumers.
	 */
	@Override
	protected void validateConfiguration() {

		super.validateConfiguration();

		Assert.state(
				!(getAcknowledgeMode().isAutoAck() && transactionManager != null),
				"The acknowledgeMode is NONE (autoack in Rabbit terms) which is not consistent with having an "
						+ "external transaction manager. Either use a different AcknowledgeMode or make sure the transactionManager is null.");

		if (this.getConnectionFactory() instanceof CachingConnectionFactory) {
			CachingConnectionFactory cf = (CachingConnectionFactory) getConnectionFactory();
			if (cf.getChannelCacheSize() < this.concurrentConsumers) {
				cf.setChannelCacheSize(this.concurrentConsumers);
				logger.warn("CachingConnectionFactory's channelCacheSize can not be less than the number of concurrentConsumers so it was reset to match: "
						+ this.concurrentConsumers);
			}
		}

	}

	private void initializeProxy() {
		if (adviceChain.length == 0) {
			return;
		}
		ProxyFactory factory = new ProxyFactory();
		for (Advice advice : getAdviceChain()) {
			factory.addAdvisor(new DefaultPointcutAdvisor(Pointcut.TRUE, advice));
		}
		factory.setProxyTargetClass(false);
		factory.addInterface(ContainerDelegate.class);
		factory.setTarget(delegate);
		proxy = (ContainerDelegate) factory.getProxy();
	}

	// -------------------------------------------------------------------------
	// Implementation of AbstractMessageListenerContainer's template methods
	// -------------------------------------------------------------------------

	/**
	 * Always use a shared Rabbit Connection.
	 */
	protected final boolean sharedConnectionEnabled() {
		return true;
	}

	/**
	 * Creates the specified number of concurrent consumers, in the form of a
	 * Rabbit Channel plus associated MessageConsumer.
	 * 
	 * @throws Exception
	 */
	@Override
	protected void doInitialize() throws Exception {
		if (!this.isExposeListenerChannel() && this.transactionManager != null) {
			logger.warn("exposeListenerChannel=false is ignored when using a TransactionManager");
		}
		initializeProxy();
	}

	@ManagedMetric(metricType = MetricType.GAUGE)
	public int getActiveConsumerCount() {
		return cancellationLock.getCount();
	}

	/**
	 * Re-initializes this container's Rabbit message consumers, if not
	 * initialized already. Then submits each consumer to this container's task
	 * executor.
	 * 
	 * @throws Exception
	 */
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		synchronized (this.consumersMonitor) {
			int newConsumers = initializeConsumers();
			if (this.consumers == null) {
				if (logger.isInfoEnabled()) {
					logger.info("Consumers were initialized and then cleared (presumably the container was stopped concurrently)");
				}
				return;
			}
			if (newConsumers <= 0) {
				if (logger.isInfoEnabled()) {
					logger.info("Consumers are already running");
				}
				return;
			}
			Set<AsyncMessageProcessingConsumer> processors = new HashSet<AsyncMessageProcessingConsumer>();
			for (BlockingQueueConsumer consumer : this.consumers) {
				AsyncMessageProcessingConsumer processor = new AsyncMessageProcessingConsumer(
						consumer);
				processors.add(processor);
				this.taskExecutor.execute(processor);
			}
			for (AsyncMessageProcessingConsumer processor : processors) {
				FatalListenerStartupException startupException = processor
						.getStartupException();
				if (startupException != null) {
					throw new AmqpIllegalStateException(
							"Fatal exception on listener startup",
							startupException);
				}
			}
		}
	}

	@Override
	protected void doStop() {
		shutdown();
		super.doStop();
	}

	@Override
	protected void doShutdown() {

		if (!this.isRunning()) {
			return;
		}

		try {
			logger.info("Waiting for workers to finish.");
			boolean finished = cancellationLock.await(shutdownTimeout,
					TimeUnit.MILLISECONDS);
			if (finished) {
				logger.info("Successfully waited for workers to finish.");
			} else {
				logger.info("Workers not finished.  Forcing connections to close.");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.warn("Interrupted waiting for workers.  Continuing with shutdown.");
		}

		synchronized (this.consumersMonitor) {
			this.consumers = null;
		}

	}

	protected int initializeConsumers() {
		int count = 0;
		synchronized (this.consumersMonitor) {
			if (this.consumers == null) {
				cancellationLock.reset();
				this.consumers = new HashSet<BlockingQueueConsumer>(
						this.concurrentConsumers);
				for (int i = 0; i < this.concurrentConsumers; i++) {
					BlockingQueueConsumer consumer = createBlockingQueueConsumer();
					this.consumers.add(consumer);
					count++;
				}
			}
		}
		return count;
	}

	@Override
	protected boolean isChannelLocallyTransacted(Channel channel) {
		return super.isChannelLocallyTransacted(channel)
				&& this.transactionManager == null;
	}

	protected BlockingQueueConsumer createBlockingQueueConsumer() {
		BlockingQueueConsumer consumer;
		String[] queues = getRequiredQueueNames();
		// There's no point prefetching less than the tx size, otherwise the
		// consumer will stall because the broker
		// didn't get an ack for delivered messages
		int actualPrefetchCount = prefetchCount > txSize ? prefetchCount
				: txSize;
		consumer = new BlockingQueueConsumer(getConnectionFactory(),
				this.messagePropertiesConverter, cancellationLock,
				getAcknowledgeMode(), isChannelTransacted(),
				actualPrefetchCount, this.defaultRequeueRejected, queues);
		return consumer;
	}

	private void restart(BlockingQueueConsumer consumer) {
		synchronized (this.consumersMonitor) {
			if (this.consumers != null) {
				try {
					// Need to recycle the channel in this consumer
					consumer.stop();
					// Ensure consumer counts are correct (another is going
					// to start because of the exception, but
					// we haven't counted down yet)
					this.cancellationLock.release(consumer);
					this.consumers.remove(consumer);
					consumer = createBlockingQueueConsumer();
					this.consumers.add(consumer);
				} catch (RuntimeException e) {
					logger.warn("Consumer failed irretrievably on restart. "
							+ e.getClass() + ": " + e.getMessage());
					// Re-throw and have it logged properly by the caller.
					throw e;
				}
				this.taskExecutor.execute(new AsyncMessageProcessingConsumer(
						consumer));
			}
		}
	}

	private boolean receiveAndExecute(final BlockingQueueConsumer consumer)
			throws Throwable {

		if (transactionManager != null) {
			try {
				return new TransactionTemplate(transactionManager,
						transactionAttribute)
						.execute(new TransactionCallback<Boolean>() {
							public Boolean doInTransaction(
									TransactionStatus status) {
								ConnectionFactoryUtils
										.bindResourceToTransaction(
												new RabbitResourceHolder(
														consumer.getChannel(),
														false),
												getConnectionFactory(), true);
								try {
									return doReceiveAndExecute(consumer);
								} catch (RuntimeException e) {
									throw e;
								} catch (Throwable e) {
									throw new WrappedTransactionException(e);
								}
							}
						});
			} catch (WrappedTransactionException e) {
				throw e.getCause();
			}
		}

		return doReceiveAndExecute(consumer);

	}

	private boolean doReceiveAndExecute(BlockingQueueConsumer consumer)
			throws Throwable {
		Channel channel = consumer.getChannel();

		for (int i = 0; i < txSize; i++) {

			logger.trace("Waiting for message from consumer.");
			Message message = consumer.nextMessage(receiveTimeout);
			if (message == null) {
				break;
			}
			try {
				executeListener(channel, message);
			} catch (ImmediateAcknowledgeAmqpException e) {
				break;
			} catch (Throwable ex) {
				if (ex instanceof ShouldRetryLocallyException) {
					if (retryStrategy.shouldRetry(
							(ShouldRetryLocallyException) ex,
							consumer.getRequeueTimes(message))) {
						consumer.requeueLocalMessage(message);
						continue;
					}
				}
				consumer.rollbackOnExceptionIfNecessary(ex);
				throw ex;
			}

		}

		return consumer.commitIfNecessary(isChannelLocallyTransacted(channel));

	}

	private Advice[] getAdviceChain() {
		return this.adviceChain;
	}

	private class AsyncMessageProcessingConsumer implements Runnable {

		private final BlockingQueueConsumer consumer;

		private final CountDownLatch start;

		private volatile FatalListenerStartupException startupException;

		public AsyncMessageProcessingConsumer(BlockingQueueConsumer consumer) {
			this.consumer = consumer;
			this.start = new CountDownLatch(1);
		}

		/**
		 * Retrieve the fatal startup exception if this processor completely
		 * failed to locate the broker resources it needed. Blocks up to 60
		 * seconds waiting for an exception to occur (but should always return
		 * promptly in normal circumstances). No longer fatal if the processor
		 * does not start up in 60 seconds.
		 * 
		 * @return a startup exception if there was one
		 * @throws TimeoutException
		 *             if the consumer hasn't started
		 * @throws InterruptedException
		 *             if the consumer startup is interrupted
		 */
		public FatalListenerStartupException getStartupException()
				throws TimeoutException, InterruptedException {
			start.await(60000L, TimeUnit.MILLISECONDS);
			return startupException;
		}

		public void run() {

			boolean aborted = false;

			try {

				try {
					consumer.start();
					start.countDown();
				} catch (FatalListenerStartupException ex) {
					throw ex;
				} catch (Throwable t) {
					start.countDown();
					handleStartupFailure(t);
					throw t;
				}

				if (SimpleMessageListenerContainer.this.transactionManager != null) {
					/*
					 * Register the consumer's channel so it will be used by the
					 * transaction manager if it's an instance of
					 * RabbitTransactionManager.
					 */
					ConnectionFactoryUtils.registerConsumerChannel(consumer
							.getChannel());
				}

				// Always better to stop receiving as soon as possible if
				// transactional
				boolean continuable = false;
				while (isActive() || continuable) {
					try {
						// Will come back false when the queue is drained
						continuable = receiveAndExecute(consumer)
								&& !isChannelTransacted();
					} catch (ListenerExecutionFailedException ex) {
						// Continue to process, otherwise re-throw
					}
				}

			} catch (InterruptedException e) {
				logger.debug("Consumer thread interrupted, processing stopped.");
				Thread.currentThread().interrupt();
				aborted = true;
			} catch (FatalListenerStartupException ex) {
				logger.error("Consumer received fatal exception on startup", ex);
				this.startupException = ex;
				// Fatal, but no point re-throwing, so just abort.
				aborted = true;
			} catch (FatalListenerExecutionException ex) {
				logger.error(
						"Consumer received fatal exception during processing",
						ex);
				// Fatal, but no point re-throwing, so just abort.
				aborted = true;
			} catch (Throwable t) {
				if (logger.isDebugEnabled()) {
					logger.warn(
							"Consumer raised exception, processing can restart if the connection factory supports it",
							t);
				} else {
					logger.warn("Consumer raised exception, processing can restart if the connection factory supports it. "
							+ "Exception summary: " + t);
				}
			} finally {
				if (SimpleMessageListenerContainer.this.transactionManager != null) {
					ConnectionFactoryUtils.unRegisterConsumerChannel();
				}
			}

			// In all cases count down to allow container to progress beyond
			// startup
			start.countDown();

			if (!isActive() || aborted) {
				logger.debug("Cancelling " + consumer);
				try {
					consumer.stop();
				} catch (AmqpException e) {
					logger.info("Could not cancel message consumer", e);
				}
				if (aborted) {
					logger.info("Stopping container from aborted consumer");
					stop();
				}
			} else {
				logger.info("Restarting " + consumer);
				restart(consumer);
			}

		}

	}

	@Override
	protected void invokeListener(Channel channel, Message message)
			throws Exception {
		proxy.invokeListener(channel, message);
	}

	/**
	 * Wait for a period determined by the {@link #setRecoveryInterval(long)
	 * recoveryInterval} to give the container a chance to recover from consumer
	 * startup failure, e.g. if the broker is down.
	 * 
	 * @param t
	 *            the exception that stopped the startup
	 * @throws Exception
	 *             if the shared connection still can't be established
	 */
	protected void handleStartupFailure(Throwable t) throws Exception {
		try {
			long timeout = System.currentTimeMillis() + recoveryInterval;
			while (isActive() && System.currentTimeMillis() < timeout) {
				Thread.sleep(200);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(
					"Unrecoverable interruption on consumer restart");
		}
	}

	@SuppressWarnings("serial")
	private static class WrappedTransactionException extends RuntimeException {
		public WrappedTransactionException(Throwable cause) {
			super(cause);
		}
	}

}
