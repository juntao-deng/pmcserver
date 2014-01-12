package net.juniper.jmp.monitor.receiver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import net.juniper.jmp.tracer.common.MonitorConstants;
import net.juniper.jmp.tracer.lock.LockHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Consumer extends Thread implements IConsumer{
	private static Logger logger = LoggerFactory.getLogger(Consumer.class);
	private RandomAccessFile raFile;
    private FileChannel raChannel;
    private MappedByteBuffer mappedBuffer;
    private Integer action;
//    private ResultObject result;
    private Integer currentRequestSeq = -1;
    boolean changedRequest = false;
    private IReceiver receiver;
//    private Integer processedSeq = -1;
    public Consumer(Integer action, IReceiver receiver){
    	try{
	    	this.action = action;
	    	this.receiver = receiver;
	    	String dir = "/var/tmp/monitor/";
	    	String fileName = dir + MonitorConstants.SHARE_CONTENT_NAME + action;
	    	raFile = new RandomAccessFile(fileName, "rw");
	    	raChannel = raFile.getChannel();
	    	mappedBuffer = raChannel.map(FileChannel.MapMode.READ_WRITE, 0, MonitorConstants.SHARE_CONTENT_SIZE).load();
    	}
        catch(IOException ex){
        	logger.error(ex.getMessage(), ex);
        }
    }
    
    public void run(){
    	logger.info("Consumer " + this.getClass().getName() + " started");
    	while(true){
//    		if(currentRequestSeq > processedSeq)
    		if(!changedRequest){
	    		try {
					Thread.sleep(10);
					continue;
				} 
	    		catch (InterruptedException e) {
	    			logger.error(e.getMessage(), e);
				}
    		}
    		changedRequest = false;
    		logger.info("### consuming for action:" + action + ",actionSeq:" + this.currentRequestSeq);
    		doConsume();
    	}
    }
    
    public void setCurrentSeq(Integer seq) {
    	if(seq <= this.currentRequestSeq)
    		return;
    	this.currentRequestSeq = seq;
    	this.changedRequest = true;
    }

    /**
     * consume content, if it's not a long time request, just wait a little while
     */
	private void doConsume() {
		//1 second
		int count = 100;
    	int i = 0;
    	boolean processed = false;
    	boolean recordLongtime = false;
    	boolean gotLock = false;
        while(i < count){
        	i ++;
//        	FileLock lock = null;
        	boolean lock = false;
            try{
            	Thread.sleep(10);
            	lock = LockHelper.getLock(mappedBuffer, LockHelper.LOCK_READ);
//                lock = raChannel.tryLock(0 , MonitorConstants.SHARE_CONTENT_SIZE, false);
                if(!lock){
                    continue;
                }
                gotLock = true;
                mappedBuffer.rewind();
                //state
                mappedBuffer.getInt();
                Integer currentAction = mappedBuffer.getInt();
                Integer actionSeq = mappedBuffer.getInt();
                if(currentAction <= 0)
                	continue;
                //overtime or processed
                if(actionSeq > 0 && actionSeq < currentRequestSeq)
                	break;
                
                //wait for long time operation, 1 minute
                if(currentAction == MonitorConstants.ACTION_PROCESSING){
                	if(recordLongtime)
                		continue;
                	else{
	                	recordLongtime = true;
	                	logger.info("### long time processing:" + this.action + "," + actionSeq);
	                	count = 6000;
	                	continue;
                	}
                }
                
                if(currentAction != this.action){
                	mappedBuffer.clear();
                	mappedBuffer.putInt(LockHelper.LOCK_READ);
                	mappedBuffer.putInt(-1);
                	mappedBuffer.putInt(-1);
                	throw new IllegalArgumentException("wrong chanel, action:" + this.action);
                }
                
                if(actionSeq.equals(currentRequestSeq)){
                	byte[] bytes = doReceive(mappedBuffer);
                	if(recordLongtime)
                		logger.info("### long time processed:" + currentAction + "," + actionSeq);
                	ResultObject result = new ResultObject(actionSeq, bytes);
                	this.receiver.setResult(action, result);
                	logger.info("### return bytes:" + result.count() + ",seq:" + actionSeq);
                	processed = true;
                	break;
                }
                mappedBuffer.clear();
                mappedBuffer.putInt(LockHelper.LOCK_READ);
                mappedBuffer.putInt(-1);
                mappedBuffer.putInt(-1);
            }
            catch(Exception ex){
            	logger.error(ex.getMessage(), ex);
            }
            finally{
            	if(lock)
            		LockHelper.releaseLock(mappedBuffer);
//            	if(lock != null){
//            		try {
//						lock.release();
//					} 
//            		catch (IOException e) {
//						logger.error(e.getMessage(), e);
//					}
//            	}
            }
        }
        if(!processed){
        	logger.info("### ----------------- consume failed, for action:" + action + ",sequence:" + currentRequestSeq + ",got lock:" + gotLock);
        	this.receiver.setResult(action, null);
        }
        else{
        	logger.info("### consume success, for action:" + action + ",sequence:" + currentRequestSeq);
        }
	}

	protected abstract byte[] doReceive(MappedByteBuffer buf);
	
//	public ResultObject getResult(Integer actionSeq) {
//		if(actionSeq == null){
//			logger.error("illegal null for actionSeq");
//			return null;
//		}
//
//		if(actionSeq < currentRequestSeq)
//			return null;
//		else if(actionSeq.equals(currentRequestSeq)){
//			if(result != null)
//				logger.info("------------------------to return:" + result.hashCode() + "," + result.count());
//			return result;
//		}
//		return SuggestWaiting.INSTANCE;
//	}
	
	protected byte[] object2Bytes(Object obj){
		ByteArrayOutputStream bout = null;
		try{
			bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(obj);
			return bout.toByteArray();
		}
		catch(Exception e){
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
	
} 