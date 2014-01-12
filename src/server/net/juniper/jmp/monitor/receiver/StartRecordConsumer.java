package net.juniper.jmp.monitor.receiver;

import java.nio.MappedByteBuffer;

import net.juniper.jmp.tracer.common.MonitorConstants;

public class StartRecordConsumer extends Consumer{
	public StartRecordConsumer(SenderConsumerFactory instance) {
		super(MonitorConstants.ACTION_RECORD, instance);
	}

	@Override
	protected byte[] doReceive(MappedByteBuffer buf) {
		return null;
	}

}
