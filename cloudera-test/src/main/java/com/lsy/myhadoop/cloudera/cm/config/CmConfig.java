package com.lsy.myhadoop.cloudera.cm.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix="cm.server")
public class CmConfig {
	private String address;
	private Integer port;
	private String username;
	private String password;
	public String getAddress() {
		return address;
	}
	public Integer getPort() {
		return port;
	}
	public String getPassword() {
		return password;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public void setPort(Integer port) {
		this.port = port;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getHostAport() {
		return new StringBuffer().append(address).append(":").append(port).toString();
	}
}
