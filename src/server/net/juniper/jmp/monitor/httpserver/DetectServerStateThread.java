package net.juniper.jmp.monitor.httpserver;

import java.io.IOException;
import java.net.SocketTimeoutException;

import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Detect if monitored server is alive
 * @author juntaod
 *
 */
public class DetectServerStateThread implements Runnable {
	private Logger logger = LoggerFactory.getLogger(DetectServerStateThread.class);
	private String etag;
	private String lastModified;
	private CloseableHttpClient httpclient = null;
	@Override
	public void run() {
		try{
			httpclient = HttpClients.createDefault();
			while(true){
				String ip = ServerConfiguration.getInstance().getConfig(ServerConfiguration.NODE_IP);
				String port = ServerConfiguration.getInstance().getConfig(ServerConfiguration.NODE_PORT);
				boolean alive = request(ip, port);
				logger.info("------------------Space node is alive:" + alive);
				ServerConfiguration.getInstance().setConfig(ServerConfiguration.NODE_ALIVE, alive + "");
				try {
					Thread.sleep(10000);
				} 
				catch (InterruptedException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		finally{
			if(httpclient != null){
				try {
					httpclient.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private boolean request(String ip, String port){
		String targetUrl = "http://" + ip + ":" + port + "/sm/css/style.css";
		HttpGet httpGet = new HttpGet(targetUrl);
		Builder builder = RequestConfig.custom();
		builder.setConnectTimeout(1000);
		builder.setConnectTimeout(1000);
		builder.setSocketTimeout(1000);
		httpGet.setConfig(builder.build());
		if(etag != null){
			httpGet.addHeader("If-None-Match", etag);
		}
		if(lastModified != null)
			httpGet.addHeader("If-Modified-Since", lastModified);
		
		CloseableHttpResponse resp = null;
		try {
			resp = httpclient.execute(httpGet);
			Header etagHeader = resp.getFirstHeader("ETag");
			if(etagHeader != null){
				etag = etagHeader.getValue();
			}
			Header modHeader = resp.getFirstHeader("Last-Modified");
			if(modHeader != null)
				lastModified = modHeader.getValue();
		    return true;
		}
		catch(ConnectTimeoutException e){
			logger.error("Connect timeout for url:" + targetUrl);
		}
		catch(HttpHostConnectException e){
			logger.error("Connect timeout for url:" + targetUrl);
		}
		catch(SocketTimeoutException e){
			logger.error("Connect timeout for url:" + targetUrl);
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		finally {
			if(resp != null){
				try {
					resp.close();
				} 
				catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return false;
	}

}
