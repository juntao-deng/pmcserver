package net.juniper.jmp.monitor.httpserver;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import net.juniper.jmp.monitor.core.InvocationInfo;
import net.juniper.jmp.monitor.receiver.SenderConsumerFactory;
import net.juniper.jmp.monitor.service.ServiceFactory;
import net.juniper.jmp.tracer.common.MonitorConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
/**
 * Simple Http Server, response for client monitor request
 * @author juntaod
 *
 */
public class PmcServer {
	private static Logger logger = LoggerFactory.getLogger(PmcServer.class);
	public static void main(String[] args) throws IOException {
		initializeByArgs(args);
		String ip = ServerConfiguration.getInstance().getConfig(ServerConfiguration.PMC_SERVER_IP);
		Integer port = Integer.valueOf(ServerConfiguration.getInstance().getConfig(ServerConfiguration.PMC_SERVER_PORT));
		HttpServer server = HttpServer.create(new InetSocketAddress(ip, port), 0);
		server.createContext("/", new ResponseHandler());
		server.setExecutor(null); // creates a default executor
		server.start();
       
        new Thread(new DetectServerStateThread()).start();
        new Thread(new LoadHistoryLogThread()).start();
        
        //pre initialize
        SenderConsumerFactory.getInstance();
        logger.info("------------------PmcServer is listening on " + ip + ":" + port + ",version:1.5");
        logger.info("------------------Space server node is " + ServerConfiguration.getInstance().getConfig(ServerConfiguration.NODE_IP) + ":" + ServerConfiguration.getInstance().getConfig(ServerConfiguration.NODE_PORT));
	}

	private static void initializeByArgs(String[] args) {
		String ip = getIpAddress();        
		if(args != null && args.length > 0){
			for(int i = 0; i < args.length; i ++){
				String arg = args[i];
				String[] pair = arg.split("=");
				if(pair.length == 2){
					if(pair[0].equals("nodeip"))
						ServerConfiguration.getInstance().setConfig(ServerConfiguration.NODE_IP, pair[1]);
					else if(pair[0].equals("nodeport"))
						ServerConfiguration.getInstance().setConfig(ServerConfiguration.NODE_PORT, pair[1]);
					else if(pair[0].equals("cleandb")){
						if(pair[1].equals("true")){
							deleteDb();
						}
					}
					else if(pair[0].equals("serverip"))
						ServerConfiguration.getInstance().setConfig(ServerConfiguration.PMC_SERVER_IP, pair[1]);
					else if(pair[0].equals("serverport"))
						ServerConfiguration.getInstance().setConfig(ServerConfiguration.PMC_SERVER_PORT, pair[1]);
				}
			}
		}
		
		if(ServerConfiguration.getInstance().getConfig(ServerConfiguration.NODE_IP) == null){
			ServerConfiguration.getInstance().setConfig(ServerConfiguration.NODE_IP, ip);
		}
		if(ServerConfiguration.getInstance().getConfig(ServerConfiguration.NODE_PORT) == null){
			ServerConfiguration.getInstance().setConfig(ServerConfiguration.NODE_PORT, "8080");
		}
		
		if(ServerConfiguration.getInstance().getConfig(ServerConfiguration.PMC_SERVER_IP) == null){
			ServerConfiguration.getInstance().setConfig(ServerConfiguration.PMC_SERVER_IP, ip);
		}
		if(ServerConfiguration.getInstance().getConfig(ServerConfiguration.PMC_SERVER_PORT) == null){
			ServerConfiguration.getInstance().setConfig(ServerConfiguration.PMC_SERVER_PORT, MonitorConstants.DEFAULT_LISTEN_PORT);
		}
		
		ServerConfiguration.getInstance().setConfig(ServerConfiguration.NODE_ALIVE, "false");
	}
    
	private static void deleteDb() {
		try{
			File f = new File(MonitorConstants.SHARE_BASE_DIR + "monitordb");
			if(f.exists() && f.isDirectory()){
				File newFile = new File(f.getAbsolutePath() + System.currentTimeMillis());
				f.renameTo(newFile);
				logger.info("Monitor db is renamed from " + f.getAbsolutePath() + " to " + newFile.getAbsolutePath());
			}
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
	}

	public static class ResponseHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
        	long startTime = System.currentTimeMillis();
        	String path = t.getRequestURI().getPath();
        	int length = 0;
        	try{
	        	if(path.equals("/dispatcher")){
	        		InputStream input = t.getRequestBody();
	        		ObjectInputStream oin = new ObjectInputStream(input);
	        		InvocationInfo info = (InvocationInfo) oin.readObject();
	        		logger.info("=======================begin to process request:ip:" + t.getRemoteAddress().getAddress().getHostAddress() + ", " + info.getClassName() + " ," + info.getMethod());
	        		byte[] result = ServiceFactory.execute(info);

	        		t.sendResponseHeaders(200, result == null ? -1 : result.length);
	        		if(result != null){
	        			length = result.length;
	        			t.getResponseBody().write(result);
	        		}
	        	
	        	}
	        	else{
	        		String helloStr = "Pmc Server is available";
	        		t.sendResponseHeaders(200, helloStr.length());
	        		t.getResponseBody().write(helloStr.getBytes());
	        	}
        	}
        	catch(Exception e){
        		logger.error(e.getMessage(), e);
//        		String msg = e.getMessage();
        		t.sendResponseHeaders(500, -1);
//        		t.getResponseBody().write(msg.getBytes());
        	}
        	finally{
        		long endTime = System.currentTimeMillis();
        		logger.info("=======================server processing finished:" + (endTime - startTime) + "ms, length: " + length + "\n\n\n");
        		t.close();
        	}
        }
    }
	
	public static String getIpAddress(){
		try{
			Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
			while (allNetInterfaces.hasMoreElements()){
				NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
				Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
				while (addresses.hasMoreElements()){
					InetAddress ip = (InetAddress) addresses.nextElement();
					if (ip != null && ip instanceof Inet4Address){
						return ip.getHostAddress();
					}
				}
			}
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		return null;
	}
}