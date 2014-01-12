package net.juniper.jmp.monitor.extractor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractInfoExtractor<T> implements IInfoExtractor<T> {
	private Logger logger = LoggerFactory.getLogger(AbstractInfoExtractor.class);
	private String baseDir;
	private String[] sufs;
	private boolean delete;
	public AbstractInfoExtractor(String baseDir, String[] sufs, boolean delete){
		this.baseDir = baseDir;
		this.sufs = sufs;
		this.delete = delete;
	}
	@Override
	public List<T> extract() {
		if(sufs == null || this.sufs.length == 0)
			return null;
		List<T> resultList = new ArrayList<T>();
		for(String suf : sufs){
			BufferedReader reader = null;
			String fileName = getFileName() + "." + suf;
			File f = new File(baseDir + "/" + fileName);
			if(!f.exists())
				continue;
			try{
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
				String line = reader.readLine();
				String p = "";
				while(line != null){
					if(line.startsWith("###callId:")){
						T t = processString(p);
						if(t != null)
							resultList.add(t);
						p = line;
					}
					else{
						p += "\n";
						p += line;
					}
					line = reader.readLine();
				}
				if(!p.equals("")){
					T t = processString(p);
					if(t != null)
						resultList.add(t);
				}
			}
			catch(Exception e){
				logger.error(e.getMessage(), e);
			}
			finally{
				if(reader != null){
					try {
						reader.close();
					} catch (IOException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
			
			if(delete)
				f.delete();
		}
		return resultList;
	}
	
	protected abstract T processString(String p);
	
	protected abstract String getFileName();
}