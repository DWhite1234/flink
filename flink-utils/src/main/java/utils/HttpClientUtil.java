package utils;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * HttpClient工具类
 */
public class HttpClientUtil {
    public static CloseableHttpClient httpClient = null;
    public static RequestConfig requestConfig = null;

    static {
        httpClient = HttpClients.createDefault();
        //超时设置
        requestConfig = RequestConfig.custom().setConnectTimeout(10000)
            .setConnectionRequestTimeout(10000)  //请求超时
            .setSocketTimeout(10000)
            .setRedirectsEnabled(true)  //允许自动重定向
            .build();
    }

    /**
     * 发送post请求
     * @param url 请求地址
     * @return
     */
    public static String doPost(String url) {

        if(url == null) {
            return null;
        }
        HttpPost httpPost = new HttpPost(url);
        httpPost.setConfig(requestConfig);
        httpPost.setHeader("Content-Type", "application/json;charset=utf-8");
        httpPost.setHeader("Accept", "application/json");
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(httpPost);
            if(httpResponse != null) {
                HttpEntity httpEntity = httpResponse.getEntity();
                if(httpEntity != null) {
                    return EntityUtils.toString(httpEntity);
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * get
     * @param url
     * @return
     */
    public static String doGet(String url){
        if(url == null) {
            return null;
        }

        HttpGet httpGet= new HttpGet(url);
        httpGet.setConfig(requestConfig);
        httpGet.setHeader("Content-Type", "application/json;charset=utf-8");
        httpGet.setHeader("Accept", "application/json");
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(httpGet);
            if(httpResponse != null) {
                HttpEntity httpEntity = httpResponse.getEntity();
                if(httpEntity != null) {
                    return EntityUtils.toString(httpEntity);
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
