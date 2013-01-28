package org.springframework.amqp.rabbit.exception;

@SuppressWarnings("serial")
public class BlockingQueueException extends RuntimeException
{
	public BlockingQueueException(String message)
	{
		super(message);
	}

	public BlockingQueueException(Throwable cause)
	{
		super(cause);
	}

	public BlockingQueueException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
