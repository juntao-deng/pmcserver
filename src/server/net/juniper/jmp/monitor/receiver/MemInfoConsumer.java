package net.juniper.jmp.monitor.receiver;

import java.nio.MappedByteBuffer;

import net.juniper.jmp.tracer.common.MonitorConstants;
import net.juniper.jmp.tracer.info.MemInfo;
/**
 * Consuming memory contents and wrapped as a MemInfo object bytes
 * @author juntaod
 *
 */
public class MemInfoConsumer extends Consumer{
	public MemInfoConsumer(IReceiver instance) {
		super(MonitorConstants.ACTION_MEMINFO, instance);
	}

	@Override
	protected byte[] doReceive(MappedByteBuffer buf) {
		MemInfo mem = new MemInfo();
		int free = buf.getInt();
		int total = buf.getInt();
		int max = buf.getInt();
		mem.setMax(max);
		mem.setTotal(total);
		mem.setFree(free);
		return object2Bytes(mem);
	}
}
