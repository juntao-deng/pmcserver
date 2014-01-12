package net.juniper.jmp.monitor.sender;

import net.juniper.jmp.tracer.common.MonitorConstants;

public class CpuInfoSender extends AbstractRequestSender {

	public CpuInfoSender() {
		super(MonitorConstants.ACTION_CPUINFO);
	}

	@Override
	protected void doSend(String requestParam) {
	}

}
