package com.lsy.myhadoop.es.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.client.AsyncRestTemplate;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.Charset;
import java.util.concurrent.Future;

/**
 * http请求工具
 * @author lutao
 *
 */
public class HttpUtils {
	private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

	/**
     * 向指定URL发送GET方法的请求
     *
     * @param url
     *            发送请求的URL
     * @param param
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return 所代表远程资源的响应结果
     */
	public static Future doGetAsync(String url, String param) {
		AsyncRestTemplate restTemplate = new AsyncRestTemplate();
    	LOG.debug("url="+url+"?"+param);
		restTemplate.getMessageConverters()
				.add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
		ListenableFuture<ResponseEntity<String>> message = restTemplate.getForEntity(url+"?"+param, String.class);
		return message;
	}
	public static String doGet(String url, String param, HttpHeaders headers) {
//		MultiValueMap<String, String> headers = new LinkedMultiValueMap<String, String>();
//		headers.add("Authorization", "Basic " + base64Creds);
//		headers.add("Content-Type", "application/json");

		RestTemplate restTemplate = new RestTemplate();
		restTemplate.getMessageConverters().add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
		HttpEntity<String> entityReq = new HttpEntity<String>(param, headers);
		ResponseEntity<String> response = restTemplate.exchange(url+"?"+param, HttpMethod.GET, entityReq, String.class);
		return response.getBody();
	}
	public static String doGet(String url, String param) {
		RestTemplate restTemplate = new RestTemplate();
//    	LOG.debug("url="+url+"?"+param);
		restTemplate.getMessageConverters()
				.add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
		String message = restTemplate.getForObject(url+"?"+param, String.class );
		return message;
	}
	public static String doGet(String url) {
		RestTemplate restTemplate = new RestTemplate();
    	LOG.debug("url="+url);
		restTemplate.getMessageConverters()
				.add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
		String message = restTemplate.getForObject(url, String.class );
//		LOG.debug(message);
		return message;
	}

	
	/**
	 * 带超时的Get请求
	 * @param url
	 * @param connectionRequestTimeout millisecond
	 * @param connectTimeout millisecond
	 * @param readTimeout millisecond
	 * @return
	 */
	public static String doGetWithTimeout(String url, int connectionRequestTimeout,
			int connectTimeout, int readTimeout) {
    	LOG.debug("url="+url);
		HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
	    httpRequestFactory.setConnectionRequestTimeout(connectionRequestTimeout);
	    httpRequestFactory.setConnectTimeout(connectTimeout);
	    httpRequestFactory.setReadTimeout(readTimeout);
		RestTemplate restTemplate = new RestTemplate(httpRequestFactory);
		restTemplate.getMessageConverters()
				.add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
		String message = restTemplate.getForObject(url, String.class );
//    	LOG.debug("resp="+message);
		return message;
	}
	
	/**
	 * 带超时的Get请求
	 * @param url
	 * @param param
	 * @param connectionRequestTimeout millisecond
	 * @param connectTimeout millisecond
	 * @param readTimeout millisecond
	 * @return
	 */
    public static String doGetWithTimeout(String url, String param,
    		 int connectionRequestTimeout, int connectTimeout, int readTimeout) {
		HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
	    httpRequestFactory.setConnectionRequestTimeout(connectionRequestTimeout);
	    httpRequestFactory.setConnectTimeout(connectTimeout);
	    httpRequestFactory.setReadTimeout(readTimeout);
		RestTemplate restTemplate = new RestTemplate(httpRequestFactory);


    	restTemplate.getMessageConverters()
        .add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
    	String message = restTemplate.getForObject(url+"?"+param, String.class );
    	return message;
    }
    /**
     * 向指定URL发送POST方法的请求(异步)
     *
     * @param url
     *            发送请求的URL
     * @param param
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return 所代表远程资源的响应结果
     */
	public static Future doPostAsync(String url, String param) {
		AsyncRestTemplate restTemplate = new AsyncRestTemplate();
		LOG.debug("url="+url+"?"+param);
		HttpHeaders headers = new HttpHeaders();
		MediaType type = MediaType.parseMediaType("application/x-www-form-urlencoded; charset=UTF-8");
		headers.setContentType(type);
//		LOG.debug(type);
		HttpEntity<String> requestEntity = new HttpEntity<String>(param,  headers);
		ListenableFuture<ResponseEntity<String>> message = restTemplate.postForEntity(url, requestEntity, String.class);
		return message;
	}

	/**
	 * 向指定URL发送POST方法的请求(同步)
	 *
	 * @param url
	 *            发送请求的URL
	 * @param param
	 *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
	 * @return 所代表远程资源的响应结果
	 */
    public static String doPost(String url, String param) {
		RestTemplate restTemplate = new RestTemplate();
		LOG.debug("---------url="+url+"?"+param);
		HttpHeaders headers = new HttpHeaders();
		MediaType type = MediaType.parseMediaType("application/x-www-form-urlencoded; charset=UTF-8");
		headers.setContentType(type);
//		LOG.debug(type);
		HttpEntity<String> requestEntity = new HttpEntity<>(param,  headers);
		String message = restTemplate.postForObject(url, requestEntity, String.class);
		return message;
	}
//	public static String doPost(String url, String param, MultiValueMap<String, String> headers) {
////		System.out.println("param="+param);
//		RestTemplate restTemplate = new RestTemplate();
////		restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
//		HttpEntity<String> requestEntity = new HttpEntity<>(param, headers);
//		ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, requestEntity, String.class);
////		System.out.println("response.getBody()="+response.getBody());
//		return response.getBody();
//	}
	public static String doPost(String url, String param, HttpHeaders headers) {
		LOG.debug("url="+url+"?"+param);
//		System.setProperty("sun.net.http.allowRestrictedHeaders", "true");	// allow header set host
		RestTemplate restTemplate = new RestTemplate();
		restTemplate.getMessageConverters().add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
		HttpEntity<String> requestEntity = new HttpEntity<>(param, headers);
		//ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, requestEntity, String.class);
		ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, requestEntity, String.class);
		System.out.println("123");
		//LOG.debug("body="+response.getBody());
		return response.getBody();
	}
	
	/**
	 * 带超时的Get请求
	 * @param url
	 * @param param
	 * @param connectionRequestTimeout millisecond
	 * @param connectTimeout millisecond
	 * @param readTimeout millisecond
	 * @return ResponseEntity
	 */
    public static ResponseEntity<String> getWithTimeout(String url, String param,
    		 int connectionRequestTimeout, int connectTimeout, int readTimeout) {
		HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
	    httpRequestFactory.setConnectionRequestTimeout(connectionRequestTimeout);
	    httpRequestFactory.setConnectTimeout(connectTimeout);
	    httpRequestFactory.setReadTimeout(readTimeout);
		RestTemplate restTemplate = new RestTemplate(httpRequestFactory);
    	restTemplate.getMessageConverters()
        .add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
    	return restTemplate.getForEntity(url+"?"+param, String.class );
    }

	/**
	 * 带超时的post请求
	 * @param url
	 * @param param
	 * @param connectionRequestTimeout millisecond
	 * @param connectTimeout millisecond
	 * @param readTimeout millisecond
	 * @return ResponseEntity
	 */
	public static ResponseEntity<String> postWithTimeout(String url, String param, 
			int connectionRequestTimeout, int connectTimeout, int readTimeout) {
		LOG.debug("url="+url+"?"+param);
		try {
			HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
			httpRequestFactory.setConnectionRequestTimeout(connectionRequestTimeout);
			httpRequestFactory.setConnectTimeout(connectTimeout);
			httpRequestFactory.setReadTimeout(readTimeout);
			RestTemplate restTemplate = new RestTemplate(httpRequestFactory);

			HttpHeaders headers = new HttpHeaders();
			MediaType type = MediaType.parseMediaType("application/x-www-form-urlencoded; charset=UTF-8");
			headers.setContentType(type);
//			LOG.debug(type);
			HttpEntity<String> requestEntity = new HttpEntity<String>(param,  headers);
			LOG.debug(requestEntity.toString());
			return restTemplate.postForEntity(url, requestEntity, String.class);
		} catch (Exception e) {
			LOG.error(url+",param:"+param);
			throw e;
		}
	}
	
	public static int doPostgetCode(String url, String param) {
		RestTemplate restTemplate = new RestTemplate();
		LOG.debug("url="+url+"?"+param);
		HttpHeaders headers = new HttpHeaders();
		MediaType type = MediaType.parseMediaType("application/x-www-form-urlencoded; charset=UTF-8");
		headers.setContentType(type);
//		LOG.debug(type);
		HttpEntity<String> requestEntity = new HttpEntity<String>(param,  headers);
		ResponseEntity responseEntity = restTemplate.postForEntity(url, requestEntity, String.class);
		LOG.debug("StatusCode="+responseEntity.getStatusCode());
		return Integer.parseInt(responseEntity.getStatusCode().toString());
	}


	public static String doPostByJson(String url, String param) {
		RestTemplate restTemplate = new RestTemplate();
		LOG.debug("url=" + url + "?" + param);
		HttpHeaders headers = new HttpHeaders();
		MediaType type = MediaType.parseMediaType("application/json");
		headers.setContentType(type);
		HttpEntity<String> requestEntity = new HttpEntity<>(param, headers);
		String message = (String)restTemplate.postForObject(url, requestEntity, String.class, new Object[0]);
		return message;
	}

	public static String doPostByJsonWithTimeout(String url, String param, 
			int connectionRequestTimeout, int connectTimeout, int readTimeout) {
		LOG.debug("url="+url+"?"+param);
		try {
			HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
			httpRequestFactory.setConnectionRequestTimeout(connectionRequestTimeout);
			httpRequestFactory.setConnectTimeout(connectTimeout);
			httpRequestFactory.setReadTimeout(readTimeout);
			RestTemplate restTemplate = new RestTemplate(httpRequestFactory);

			HttpHeaders headers = new HttpHeaders();
			MediaType type = MediaType.parseMediaType("application/json");
			headers.setContentType(type);
			HttpEntity<String> requestEntity = new HttpEntity<>(param,  headers);
			String message = (String)restTemplate.postForObject(url, requestEntity, String.class, new Object[0]);
//			LOG.debug(message);
			return message;
		} catch (Exception e) {
			LOG.error(url+",param:"+param);
			throw e;
		}
	}

//
//    /**
//     * 向指定URL发送GET方法的请求
//     *
//     * @param url
//     *            发送请求的URL
//     * @param param
//     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
//     * @return URL 所代表远程资源的响应结果
//     */
//    public static String sendGet(String url, String param) {
//        String result = "";
//        BufferedReader in = null;
//        try {
//            String urlNameString = url + "?" + param;
//            URL realUrl = new URL(urlNameString);
//            // 打开和URL之间的连接
//            URLConnection connection = realUrl.openConnection();
//            // 设置通用的请求属性
//            connection.setRequestProperty("accept", "*/*");
//            connection.setRequestProperty("connection", "Keep-Alive");
//            connection.setRequestProperty("user-agent",
//                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
////            connection.addRequestProperty("Content-Type", "text/html;charset=UTF-8");
//            // 建立实际的连接
//            connection.connect();
//            // 获取所有响应头字段
//            Map<String, List<String>> map = connection.getHeaderFields();
//            // 遍历所有的响应头字段
//            for (String key : map.keySet()) {
//                LOG.debug(key + "--->" + map.get(key));
//            }
//            // 定义 BufferedReader输入流来读取URL的响应
//            in = new BufferedReader(new InputStreamReader(
//                    connection.getInputStream()));
//            String line;
//            while ((line = in.readLine()) != null) {
//                result += line;
//            }
////            result = new String(result.getBytes("ISO-8859-1"), "utf-8");
//        } catch (Exception e) {
//            LOG.debug("发送GET请求出现异常！" + e);
//            e.printStackTrace();
//        }
//        // 使用finally块来关闭输入流
//        finally {
//            try {
//                if (in != null) {
//                    in.close();
//                }
//            } catch (Exception e2) {
//                e2.printStackTrace();
//            }
//        }
//        return result;
//    }
//
//    /**
//     * 向指定 URL 发送POST方法的请求
//     *
//     * @param url
//     *            发送请求的 URL
//     * @param param
//     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
//     * @return 所代表远程资源的响应结果
//     */
//    public static String sendPost(String url, String param) {
//        PrintWriter out = null;
//        BufferedReader in = null;
//        String result = "";
//        try {
//            URL realUrl = new URL(url);
//            // 打开和URL之间的连接
//            URLConnection conn = realUrl.openConnection();
//            // 设置通用的请求属性
//            conn.setRequestProperty("accept", "*/*");
//            conn.setRequestProperty("connection", "Keep-Alive");
//            conn.setRequestProperty("user-agent",
//                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
////            conn.setRequestProperty("content-type", "text/html;charset=utf-8");
//            // 发送POST请求必须设置如下两行
//            conn.setDoOutput(true);
//            conn.setDoInput(true);
//            // 获取URLConnection对象对应的输出流
//            out = new PrintWriter(conn.getOutputStream());
//            // 发送请求参数
//            out.print(param);
//            // flush输出流的缓冲
//            out.flush();
//            // 定义BufferedReader输入流来读取URL的响应
//            in = new BufferedReader(
//                    new InputStreamReader(conn.getInputStream()));
//            String line;
//            while ((line = in.readLine()) != null) {
//                result += line;
//            }
////            result = new String(result.getBytes("ISO-8859-1"), "utf-8");
//        } catch (Exception e) {
//            LOG.debug("发送 POST 请求出现异常！"+e);
//            e.printStackTrace();
//        }
//        //使用finally块来关闭输出流、输入流
//        finally{
//            try{
//                if(out!=null){
//                    out.close();
//                }
//                if(in!=null){
//                    in.close();
//                }
//            }
//            catch(IOException ex){
//                ex.printStackTrace();
//            }
//        }
//        return result;
//    }


}