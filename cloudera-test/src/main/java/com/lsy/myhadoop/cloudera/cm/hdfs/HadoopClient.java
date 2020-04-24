package com.lsy.myhadoop.cloudera.cm.hdfs;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.v14.RootResourceV14;
import com.lsy.myhadoop.cloudera.cm.config.CmConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;


public class HadoopClient {
	private static final Logger logger = LoggerFactory.getLogger(HadoopClient.class);

	private FileSystem fileSystem;
	private YarnClient yarnClient;
	
//	@Value("${hdfs.proxy.user:hdfs}")
	private String hdfsSuperUser;

	public RootResourceV14 rootResource(CmConfig config) {
		logger.info("cm addr:{}:{}",config.getAddress(),config.getPort());
		return new ClouderaManagerClientBuilder()
				.withHost(config.getAddress())
				.withPort(config.getPort())
				.withUsernamePassword(config.getUsername(),config.getPassword())
				.build()
				.getRootV14();
	}

	public FileSystem getFileSystem() {
		if(fileSystem==null) {
			throw new RuntimeException("FileSytem not init yet");
		}
		return fileSystem;
	}

	public YarnClient getYarnClient() {
		if(fileSystem==null) {
			throw new RuntimeException("YarnClient not init yet");
		}
		return yarnClient;
	}

	public synchronized void initFileSystem(Configuration config) throws InterruptedException {
		try {
			String username = System.getProperty("user.name");
			//hdfs 中root用户不允许模仿成其他用户 这里就用root用户启动
			if("root".equals(username)) {
				fileSystem =FileSystem.get(config);
			}
			//其他用户 模拟成可配置的${hdfs.proxy.user} 
			else {
				UserGroupInformation superUser = UserGroupInformation.getCurrentUser();
				UserGroupInformation proxyUgi = UserGroupInformation.createProxyUser(hdfsSuperUser, superUser);
				proxyUgi.doAs(new PrivilegedExceptionAction<Void>() {
					@Override
					public Void run() throws Exception {
						fileSystem =FileSystem.get(config);
						return null;
					}
				});
			}
			logger.info("hdfs client success");
		} catch (IOException e) {
			logger.error("init HDFS client fail,{}",e.getMessage());
		}
	}
	
	public static void main(String[] args) {
		System.getProperties().forEach((k, v)->{
			System.out.println(k+"--->"+v);
		});
	}
	
	public synchronized void initYarnClient(Configuration config) {
		yarnClient = new YarnClientImpl();
		yarnClient.init(config);
		yarnClient.start();
		logger.info("yarn client start success"+ yarnClient.getServiceState());
	}

//	@PreDestroy
	public void close() {
		logger.info("close hadoop client's connections.");
		close(fileSystem);
		close(yarnClient);
	}

	public static void close(Closeable resource) {
		if (resource != null) {
			try {
				resource.close();
			} catch (Exception e) {
				logger.error("close {} fail,{}", resource, e.getMessage());
			}
		}
	}
	
}
