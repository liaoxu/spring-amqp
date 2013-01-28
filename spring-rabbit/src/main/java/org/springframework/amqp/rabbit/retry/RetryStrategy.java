package org.springframework.amqp.rabbit.retry;

public interface RetryStrategy
{
	/**
	 * 返回在指定的异常和重试次数后是否需要继续尝试操作
	 * @param e
	 * @param numOperationInvocations
	 * @return
	 */
	public boolean shouldRetry(Exception e, int numOperationInvocations);
}
