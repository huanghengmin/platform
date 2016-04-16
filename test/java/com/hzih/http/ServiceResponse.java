package com.hzih.http;

import java.io.InputStream;

/**
 * 服务响应
 * 
 * @author collin.code@gmail.com
 * 
 */
public class ServiceResponse {
	/**
	 * 响应代码：200 400 500
	 */
	int code;

	/**
	 * json响应数据
	 */
	String data;

    InputStream inputStream;

	public ServiceResponse(int code, String data) {
		super();
		this.code = code;
		this.data = data;
	}

	public ServiceResponse() {

	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

    public InputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }
}
