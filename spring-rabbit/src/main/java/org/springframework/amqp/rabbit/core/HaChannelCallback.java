package org.springframework.amqp.rabbit.core;

import com.rabbitmq.client.Channel;

public interface HaChannelCallback<T> extends ChannelCallback<T> {
	/**
	 * Execute any number of operations by java BlockingQueue instead of
	 * RabbitMQ {@link Channel}, possibly returning a result.
	 */
	T doInBlockingQueue() throws Exception;
}
