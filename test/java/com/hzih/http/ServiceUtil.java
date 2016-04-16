package com.hzih.http;

import com.hzih.http.ServiceResponse;
//import org.apache.commons.httpclient.HttpClient;
//import org.apache.commons.httpclient.methods.PostMethod;
//import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * 调用服务接口封装
 * 
 * @author collin.code@gmail.com
 * 
 */
public class ServiceUtil {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ServiceUtil.class);





    public static ServiceResponse callManager(String[][] params,String managerUrl) {
        HttpClient client = new HttpClient();
		client.getHttpConnectionManager().getParams().setConnectionTimeout(5 * 1000);
		client.getHttpConnectionManager().getParams().setSoTimeout(5 * 1000);

		PostMethod post = new PostMethod(managerUrl);
		post.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 5 * 1000);
		post.addRequestHeader("Content-Type",
				"application/x-www-form-urlencoded;charset=UTF-8");

		for (String[] param : params) {
			post.addParameter(param[0], param[1]);
		}

		ServiceResponse response = new ServiceResponse();

		int statusCode = 0;
		try {
			statusCode = client.executeMethod(post);
			//logger.info("statusCode:" + statusCode);
			response.setCode(statusCode);
			if (statusCode == 200) {
				String data = post.getResponseBodyAsString();
				//logger.info("data:" + data);
				response.setData(data);
			}
		} catch (Exception e) {
			logger.error("访问接口失败", e);
		}
		return response;
    }
}
