package org.springframework.amqp.rabbit.retry;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * 总是重试
 * 
 * @author liaoxu
 * 
 */
public class AlwaysRetryStrategy implements RetryStrategy {

	private static final Logger LOG = Logger
			.getLogger(AlwaysRetryStrategy.class);

	/**
	 * Default value = 500 = 0.5 seconds
	 */
	public static final long DEFAULT_OPERATION_RETRY_TIMEOUT_MILLIS = 500;

	private long operationRetryTimeoutMillis = DEFAULT_OPERATION_RETRY_TIMEOUT_MILLIS;

	public void setOperationRetryTimeoutMillis(final long timeout) {
		operationRetryTimeoutMillis = timeout;
	}

	public boolean shouldRetry(Exception e, int numOperationInvocations) {
		if (operationRetryTimeoutMillis > 0) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Sleeping before next operation invocation (millis): "
						+ operationRetryTimeoutMillis);
			}

			try {
				TimeUnit.MILLISECONDS.sleep(operationRetryTimeoutMillis);
			} catch (InterruptedException ie) {
				LOG.warn("Interrupted during timeout waiting for next operation invocation to occurr. "
						+ "Retrying invocation now.");
			}
		} else {

			if (LOG.isDebugEnabled()) {
				LOG.debug("No timeout set, retrying immediately");
			}
		}
		return true;
	}

}
