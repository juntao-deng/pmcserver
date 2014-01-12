package net.juniper.jmp.monitor.receiver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.juniper.jmp.monitor.sender.CpuInfoSender;
import net.juniper.jmp.monitor.sender.IRequestSender;
import net.juniper.jmp.monitor.sender.MemInfoSender;
import net.juniper.jmp.monitor.sender.RecordEndSender;
import net.juniper.jmp.monitor.sender.RecordStartSender;
import net.juniper.jmp.monitor.sender.StateInfoSender;
import net.juniper.jmp.monitor.sender.ThreadInfoSender;
import net.juniper.jmp.tracer.common.MonitorConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * a factory for signal sender and consumer from share memory channel
 * @author juntaod
 *
 */
public class SenderConsumerFactory implements IReceiver{
	private static Logger logger = LoggerFactory.getLogger(SenderConsumerFactory.class);
	private Map<Integer, IConsumer> receiverMap = new HashMap<Integer, IConsumer>();
	private Map<Integer, IRequestSender> senderMap = new HashMap<Integer, IRequestSender>();
	private static SenderConsumerFactory instance = new SenderConsumerFactory();
	private Map<Integer, ResultObject> resultMap = new ConcurrentHashMap<Integer, ResultObject>();
	
	private SenderConsumerFactory() {
		resultMap.put(MonitorConstants.ACTION_THREADINFO, SuggestWaiting.INSTANCE);
		resultMap.put(MonitorConstants.ACTION_CPUINFO, SuggestWaiting.INSTANCE);
		resultMap.put(MonitorConstants.ACTION_MEMINFO, SuggestWaiting.INSTANCE);
		resultMap.put(MonitorConstants.ACTION_STATE, SuggestWaiting.INSTANCE);
		resultMap.put(MonitorConstants.ACTION_RECORD, SuggestWaiting.INSTANCE);
		resultMap.put(MonitorConstants.ACTION_ENDRECORD, SuggestWaiting.INSTANCE);
		
		Thread receiver = new ThreadInfoConsumer(this); 
		receiverMap.put(MonitorConstants.ACTION_THREADINFO, (IConsumer) receiver);
		receiver.start();
		
		receiver = new CpuInfoConsumer(this); 
		receiverMap.put(MonitorConstants.ACTION_CPUINFO, (IConsumer) receiver);
		receiver.start();
		
		receiver = new MemInfoConsumer(this); 
		receiverMap.put(MonitorConstants.ACTION_MEMINFO, (IConsumer) receiver);
		receiver.start();
		
		
		receiver = new StateInfoConsumer(this);
		receiverMap.put(MonitorConstants.ACTION_STATE, (IConsumer) receiver);
		receiver.start();
		
		receiver = new StartRecordConsumer(this);
		receiverMap.put(MonitorConstants.ACTION_RECORD, (IConsumer) receiver);
		receiver.start();
		
		receiver = new EndRecordConsumer(this);
		receiverMap.put(MonitorConstants.ACTION_ENDRECORD, (IConsumer) receiver);
		receiver.start();
		
		senderMap.put(MonitorConstants.ACTION_THREADINFO, new ThreadInfoSender());
		senderMap.put(MonitorConstants.ACTION_CPUINFO, new CpuInfoSender());
		senderMap.put(MonitorConstants.ACTION_MEMINFO, new MemInfoSender());
		senderMap.put(MonitorConstants.ACTION_STATE, new StateInfoSender());
		senderMap.put(MonitorConstants.ACTION_RECORD, new RecordStartSender());
		senderMap.put(MonitorConstants.ACTION_ENDRECORD, new RecordEndSender());
	}
	
	public static SenderConsumerFactory getInstance() {
		return instance;
	}

	public Integer sendRequest(int action, String requestParam){
		return senderMap.get(action).send(requestParam);
	}
	
	public byte[] getResponse(Integer action, Integer actionSeq) {
		try{
			if(actionSeq == null){
				logger.error("haven't sent signal for aciton:" + action);
				return null;
			}
			IConsumer receiver = receiverMap.get(action);
			receiver.setCurrentSeq(actionSeq);
			
			ResultObject result =  SuggestWaiting.INSTANCE;
			int times = 0;
			//60 seconds
			while(result == SuggestWaiting.INSTANCE && (++ times) < 6000){
				Thread.sleep(10);
				result = resultMap.get(action);
			}
			if(result == null || result == SuggestWaiting.INSTANCE || result == NullResultObject.INSTANCE){
				return null;
			}
			if(!result.getActionReq().equals(actionSeq)){
				return null;
			}

			return result.getResult();
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
			return null;
		}
		finally{
			logger.info("### receive finished for action:" + action + ",seq:" + actionSeq);
			resultMap.put(action, SuggestWaiting.INSTANCE);
		}
	}

	@Override
	public void setResult(Integer action, ResultObject result) {
		if(result == null)
			result = NullResultObject.INSTANCE;
		resultMap.put(action, result);
	}
}
