package org.springframework.amqp.rabbit.retry;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * 
 * @author liaoxu
 * 
 */
public class TemplateRetryStrategy implements RetryStrategy {

	private static final Logger LOG = Logger
			.getLogger(TemplateRetryStrategy.class);

	/**
	 * Default value = 10000 = 10 seconds
	 */
	public static final long DEFAULT_OPERATION_RETRY_TIMEOUT_MILLIS = 500;

	/**
	 * Default value = 2 (one retry)
	 */
	public static final int DEFAULT_MAX_OPERATION_INVOCATIONS = 2;

	private long operationRetryTimeoutMillis = DEFAULT_OPERATION_RETRY_TIMEOUT_MILLIS;

	private int maxOperationInvocations = DEFAULT_MAX_OPERATION_INVOCATIONS;

	public void setMaxOperationInvocations(final int maxOperationInvocations) {
		this.maxOperationInvocations = maxOperationInvocations;
	}

	public void setOperationRetryTimeoutMillis(final long timeout) {
		operationRetryTimeoutMillis = timeout;
	}

	public boolean shouldRetry(final Exception e,
			final int numOperationInvocations) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Operation invocation failed on IOException: numOperationInvocations="
					+ numOperationInvocations
					+ ", maxOperationInvocations="
					+ maxOperationInvocations + ", message=" + e.getMessage());
		}

		if (numOperationInvocations == maxOperationInvocations) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Max number of operation invocations reached, not retrying: "
						+ maxOperationInvocations);
			}

			return false;
		}

		if (!(e instanceof IOException)) {
			return false;
		}

		if (operationRetryTimeoutMillis > 0) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Sleeping before next operation invocation (millis): "
						+ operationRetryTimeoutMillis);
			}

			try {
				Thread.sleep(operationRetryTimeoutMillis);
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
