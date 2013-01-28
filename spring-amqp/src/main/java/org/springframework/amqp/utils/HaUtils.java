package org.springframework.amqp.utils;

import java.io.EOFException;
import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;

/**
 * Utility class for HA operations.
 * 
 * @author Josh Devins
 */
public class HaUtils {

	private HaUtils() {
		// do not instantiate
	}

	/**
	 * Pulls out the cause of the {@link IOException} and if it is of type
	 * {@link ShutdownSignalException}, passes on to
	 * {@link #isShutdownRecoverable(ShutdownSignalException)}.
	 */
	public static boolean isShutdownRecoverable(final IOException ioe) {
		if (ioe.getCause() instanceof ShutdownSignalException) {
			return isShutdownRecoverable((ShutdownSignalException) ioe
					.getCause());
		}
		return true;
	}

	/**
	 * Determines if the {@link ShutdownSignalException} can be recovered from.
	 * 
	 * Straight code copy from RabbitMQ messagepatterns library v0.1.3
	 * {@code ConnectorImpl}.
	 * 
	 * <p>
	 * Changes:
	 * <ul>
	 * <li>added AlreadyClosedException as recoverable when
	 * isInitiatedByApplication == true</li>
	 * </ul>
	 * </p>
	 */
	public static boolean isShutdownRecoverable(final ShutdownSignalException s) {
		if (s != null) {
			int replyCode = 0;

			if (s.getReason() instanceof AMQImpl.Connection.Close) {
				replyCode = ((AMQImpl.Connection.Close) s.getReason())
						.getReplyCode();
			}

			if (s.isInitiatedByApplication()) {

				return replyCode == AMQP.CONNECTION_FORCED
						|| replyCode == AMQP.INTERNAL_ERROR
						|| s.getCause() instanceof EOFException
						|| s instanceof AlreadyClosedException;
			}
		}
		return false;
	}

}
