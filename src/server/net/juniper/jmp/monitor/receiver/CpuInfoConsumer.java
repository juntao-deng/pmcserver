package net.juniper.jmp.monitor.receiver;

import java.nio.MappedByteBuffer;

import net.juniper.jmp.tracer.common.MonitorConstants;
import net.juniper.jmp.tracer.info.CpuInfo;

public class CpuInfoConsumer extends Consumer{
	public CpuInfoConsumer(SenderConsumerFactory instance) {
		super(MonitorConstants.ACTION_CPUINFO, instance);
	}

	@Override
	protected byte[] doReceive(MappedByteBuffer buf) {
		CpuInfo cpu = new CpuInfo();
		float usage = buf.getFloat();
		cpu.setUsage(usage);
		return object2Bytes(cpu);
	}

}
