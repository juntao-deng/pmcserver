package net.juniper.jmp.monitor.sender;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import net.juniper.jmp.tracer.common.MonitorConstants;

public abstract class AbstractRequestSender implements IRequestSender{
	private RandomAccessFile raFile;
    private FileChannel raChannel;
    private MappedByteBuffer mappedBuffer;
    private Integer currentSeq = -1;
    private Integer action;
    public AbstractRequestSender(Integer action){
    	 try{
    		 this.action = action;
    		 String dir = MonitorConstants.SHARE_BASE_DIR;
         	 String fileName = dir + MonitorConstants.SHARE_SIGN_NAME + action;
             raFile = new RandomAccessFile(fileName, "rw");
             raChannel = raFile.getChannel();
             mappedBuffer = raChannel.map(FileChannel.MapMode.READ_WRITE, 0, MonitorConstants.SHARE_SIGN_SIZE).load();
         }
         catch(IOException ex){
             ex.printStackTrace();
         }
    }

    public Integer send(String requestParam){
    	int times = 0;
        while(times < 10){
        	FileLock lock = null;
            try{
            	if(times != 0)
            		Thread.sleep(10);
            	times ++;
                lock = raChannel.tryLock(0 , MonitorConstants.SHARE_SIGN_SIZE, false);
                if(lock == null){
                    continue;
                }
                mappedBuffer.clear();
                
                currentSeq ++;
                mappedBuffer.putInt(action);
                mappedBuffer.putInt(currentSeq);
                if(requestParam != null && !requestParam.equals("")){
                	byte[] bytes = requestParam.getBytes();
                	mappedBuffer.putInt(bytes.length);
                	mappedBuffer.put(bytes);
                }
                else{
                	mappedBuffer.putInt(0);
                }
                doSend(requestParam);
                return currentSeq;
            }
            catch(Exception ex){
                ex.printStackTrace();
            }
            finally{
            	if(lock != null){
					try {
						lock.release();
					} catch (IOException e) {
						e.printStackTrace();
					}
            	}
            }
        }
        return null;
    }

	protected abstract void doSend(String requestParam);
} 