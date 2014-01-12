package net.juniper.jmp.monitor.sender;

import net.juniper.jmp.tracer.common.MonitorConstants;

public class RecordEndSender extends AbstractRequestSender {

	public RecordEndSender() {
		super(MonitorConstants.ACTION_ENDRECORD);
	}

	@Override
	protected void doSend(String requestParam) {
		
	}

}
