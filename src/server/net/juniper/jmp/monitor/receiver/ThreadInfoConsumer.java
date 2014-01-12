package net.juniper.jmp.monitor.receiver;

import java.nio.MappedByteBuffer;

import net.juniper.jmp.tracer.common.MonitorConstants;

import org.apache.log4j.Logger;

/**
 * 
 * @author juntaod
 *
 */
public class ThreadInfoConsumer extends Consumer{
	private Logger logger = Logger.getLogger(ThreadInfoConsumer.class);
	public ThreadInfoConsumer(IReceiver instance) {
		super(MonitorConstants.ACTION_THREADINFO, instance);
	}

	@Override
	protected byte[] doReceive(MappedByteBuffer buf) {
		int length = buf.getInt();
		logger.info("-----------------------------------get thread info size:" + length);
		if(length == 0)
			return null;
		byte[] bytes = new byte[length];
		buf.get(bytes, 0, length);
		return bytes;
	}
}
