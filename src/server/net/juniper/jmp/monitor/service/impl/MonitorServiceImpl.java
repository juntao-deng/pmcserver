package net.juniper.jmp.monitor.service.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

import net.juniper.jmp.monitor.extractor.ThreadExtractor;
import net.juniper.jmp.monitor.httpserver.ServerConfiguration;
import net.juniper.jmp.monitor.receiver.SenderConsumerFactory;
import net.juniper.jmp.monitor.service.ILogService;
import net.juniper.jmp.monitor.service.IMonitorService;
import net.juniper.jmp.tracer.common.MonitorConstants;
import net.juniper.jmp.tracer.dumper.info.SqlInfoDump;
import net.juniper.jmp.tracer.dumper.info.StageInfoBaseDump;
import net.juniper.jmp.tracer.dumper.info.ThreadInfoDump;

import org.apache.log4j.Logger;
/**
 * 
 * @author juntaod
 *
 */
public class MonitorServiceImpl implements IMonitorService {
	private Logger logger = Logger.getLogger(MonitorServiceImpl.class);
	private ILogService logService;
	@Override
	public byte[] getThreadInfos() {
		if(isNodeAlive()){
			Integer actionSeq = SenderConsumerFactory.getInstance().sendRequest(MonitorConstants.ACTION_THREADINFO, null);
			byte[] bytes = (byte[]) SenderConsumerFactory.getInstance().getResponse(MonitorConstants.ACTION_THREADINFO, actionSeq);
			if(bytes != null)
				logger.error("-----------------------------trans:" + bytes.length);
			return bytes;
		}
		return null;
	}
	
	@Override
	public byte[] getPeriodThreadInfos(String startTs, String endTs, String wherePart) {
		ObjectOutputStream oout = null;
		try{
			ThreadInfoDump[] result = getLogService().getThreadInfosByPeriod(startTs, endTs, wherePart);
			if(result != null && result.length > 0){
				ByteArrayOutputStream bout = new ByteArrayOutputStream();
				oout = new ObjectOutputStream(bout);
				oout.writeObject(result);
				return bout.toByteArray();
			}
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		finally{
			if(oout != null){
				try {
					oout.close();
				} 
				catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return null;
	}


	@Override
	public byte[] getStagesByParentId(String pid) {
//		if(isNodeAlive()){
			ObjectOutputStream oout = null;
			try{
				StageInfoBaseDump[] result = getLogService().getStagesByParentId(pid);
				if(result != null && result.length > 0){
					ByteArrayOutputStream bout = new ByteArrayOutputStream();
					oout = new ObjectOutputStream(bout);
					oout.writeObject(result);
					return bout.toByteArray();
				}
			}
			catch(Exception e){
				logger.error(e.getMessage(), e);
			}
			finally{
				if(oout != null){
					try {
						oout.close();
					} 
					catch (IOException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
//		}
		return null;
	}
	
	private ILogService getLogService() {
		if(logService == null){
			logService = new LogServiceImpl();
		}
		return logService;
	}
	
	@Override
	public byte[] getCpuInfo() {
		if(isNodeAlive()){
			Integer actionSeq = SenderConsumerFactory.getInstance().sendRequest(MonitorConstants.ACTION_CPUINFO, null);
			return (byte[]) SenderConsumerFactory.getInstance().getResponse(MonitorConstants.ACTION_CPUINFO, actionSeq);
		}
		return null;
	}

	@Override
	public byte[] getMemInfo() {
		if(isNodeAlive()){
			Integer actionSeq = SenderConsumerFactory.getInstance().sendRequest(MonitorConstants.ACTION_MEMINFO, null);
			return (byte[]) SenderConsumerFactory.getInstance().getResponse(MonitorConstants.ACTION_MEMINFO, actionSeq);
		}
		return null;
	}

	@Override
	public byte[] getState() {
		if(isNodeAlive()){
			Integer actionSeq = SenderConsumerFactory.getInstance().sendRequest(MonitorConstants.ACTION_STATE, null);
			return (byte[]) SenderConsumerFactory.getInstance().getResponse(MonitorConstants.ACTION_STATE, actionSeq);
		}
		return null;
	}

	@Override
	public byte[] getServerState() {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject("ok");
			return bout.toByteArray();
		} 
		catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		finally{
			if(bout != null){
				try {
					bout.close();
				} 
				catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return null;
	}

	private boolean isNodeAlive() {
		return ServerConfiguration.getInstance().getConfig(ServerConfiguration.NODE_ALIVE).equals("true");
	}
	
	@Override
	public byte[] getNodeState() {
		if(isNodeAlive()){
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			try {
				ObjectOutputStream oout = new ObjectOutputStream(bout);
				oout.writeObject("ok");
				return bout.toByteArray();
			} 
			catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
			finally{
				if(bout != null){
					try {
						bout.close();
					} 
					catch (IOException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		}
		return null;
	}

	@Override
	public byte[] startRecord(String recordId) {
		if(isNodeAlive()){
			Integer actionSeq = SenderConsumerFactory.getInstance().sendRequest(MonitorConstants.ACTION_RECORD, recordId);
			return (byte[]) SenderConsumerFactory.getInstance().getResponse(MonitorConstants.ACTION_RECORD, actionSeq);
		}
		return null;
	}

	@Override
	public byte[] endRecord(String recordId) {
		if(isNodeAlive()){
			SenderConsumerFactory.getInstance().sendRequest(MonitorConstants.ACTION_ENDRECORD, recordId);
		}
		return null;
	}

	@Override
	public byte[] getRecordResult(String recordId) {
		if(isNodeAlive()){
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			try {
				String baseDir = MonitorConstants.PMC_LOG_DIR + "/action/";
				String file = MonitorConstants.PMC_THREAD_LOG + "." + recordId;
				List<ThreadInfoDump> result = ThreadExtractor.getInstance().extractAll(baseDir, file, recordId, true);
				if(result != null){
					ObjectOutputStream oout = new ObjectOutputStream(bout);
					oout.writeObject(result);
					return bout.toByteArray();
				}
			} 
			catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
			finally{
				if(bout != null){
					try {
						bout.close();
					} 
					catch (IOException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		}
		return null;
	}

	@Override
	public byte[] getSqlInfos(String startTs, String endTs, String wherePart) {
		if(isNodeAlive()){
			ObjectOutputStream oout = null;
			try{
				SqlInfoDump[] result = getLogService().getSqlInfos(startTs, endTs, wherePart);
				if(result != null && result.length > 0){
					ByteArrayOutputStream bout = new ByteArrayOutputStream();
					oout = new ObjectOutputStream(bout);
					oout.writeObject(result);
					return bout.toByteArray();
				}
			}
			catch(Exception e){
				logger.error(e.getMessage(), e);
			}
			finally{
				if(oout != null){
					try {
						oout.close();
					} 
					catch (IOException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		}
		return null;
	}

	@Override
	public byte[] getStageById(String callId) {
		if(isNodeAlive()){
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			try {
				StageInfoBaseDump stage = getLogService().getStageById(callId);
				if(stage != null){
					ObjectOutputStream oout = new ObjectOutputStream(bout);
					oout.writeObject(stage);
					return bout.toByteArray();
				}
			} 
			catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
			finally{
				if(bout != null){
					try {
						bout.close();
					} 
					catch (IOException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		}
		return null;
	}

}
