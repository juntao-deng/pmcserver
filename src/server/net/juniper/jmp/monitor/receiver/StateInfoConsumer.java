package net.juniper.jmp.monitor.receiver;

import java.nio.MappedByteBuffer;

import net.juniper.jmp.tracer.common.MonitorConstants;
import net.juniper.jmp.tracer.info.NodeStateInfo;
/**
 * 
 * @author juntaod
 *
 */
public class StateInfoConsumer extends Consumer{
	public StateInfoConsumer(IReceiver instance) {
		super(MonitorConstants.ACTION_STATE, instance);
	}
	
	@Override
	protected byte[] doReceive(MappedByteBuffer buf) {
		int state = buf.getInt();
		NodeStateInfo si = new NodeStateInfo();
		si.setAlive(state == 1);
		return this.object2Bytes(si);
	}

}
