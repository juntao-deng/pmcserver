package net.juniper.jmp.monitor.receiver;

import java.nio.MappedByteBuffer;

import net.juniper.jmp.tracer.common.MonitorConstants;

public class EndRecordConsumer extends Consumer{
	public EndRecordConsumer(SenderConsumerFactory instance) {
		super(MonitorConstants.ACTION_ENDRECORD, instance);
	}

	@Override
	protected byte[] doReceive(MappedByteBuffer buf) {
		byte[] bytes = new byte[6];
		buf.get(bytes, 0, 6);
		return bytes;
	}

}
