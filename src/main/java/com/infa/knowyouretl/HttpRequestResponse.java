package com.infa.knowyouretl;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * <h1> HttpRequestResponse </h1>
 * <p>
 * This class will send the request to the server and get the response back from
 * the server.</br>
 * This class uses {@link java.net.HttpURLConnection} object to establish the
 * connection to an end-point server.</br>
 * Also multiple calls can be made from the same "HttpRequestResponse" object as
 * the buffer is flushed everytime before the call is made to the server.</br>
 * </br>
 * <b>[Note : If the server request fails with 403 or some other codes then
 * response data will not be stored.</b></p>
 *
 * <p>
 * Below is the example </p>
 * <pre>
 * {@code
 *  HttpRequestResponse hrr = new HttpRequestResponse();
 *  hrr.setUrl("https://www.google.com");	//Set the URL
 *  hrr.setMethod("GET");					//Set the method GET/POST
 *
 *  Map<String, String> headers = new HashMap<String,String>(); // Create a map and add headers in key-value pair
 *  headers.put("Accept","text/html");
 *
 *  hrr.setHeaders(headers);				//Set the headers
 *  hrr.makeCall();							//Call endpoint server.
 *
 *  if(hrr.getUrlStatus()==200)				//Check the response code from the server.
 *  {
 *  	System.out.println(hrr.getResponse());	// Print the output
 *  }
 *
 * }
 * </pre>
 *
 * @author Kartik
 * @version 1.0
 */
public class HttpRequestResponse implements AutoCloseable {

    private final int CONNECTION_TIMEOUT = 180 * 1000;
    private short urlStatus;
    private String method;
    private Map<String, String> headers = new HashMap<String, String>();
    private StringBuffer response = new StringBuffer();
    private String requestData;
    private String url;
    private StringBuffer errorMessage = new StringBuffer();
    HttpURLConnection connection = null;

    /**
     * <p>
     * The HttpRequestResponse object is instantiated.</p>
     */
    public HttpRequestResponse() {
    }

    /**
     * <p>
     * This method returns the method type of the request "GET"/"POST" set by
     * {@link #setMethod(String)} method</p>
     *
     * @return method "GET"/"POST"
     */
    public String getMethod() {
        return method;
    }

    /**
     * <p>
     * This method sets the request type "GET"/"POST" accordingly.</p>
     *
     * @param method "GET"/"POST"
     */
    public void setMethod(String method) {
        //System.out.println(String.format("Method type : %s", method));
        this.method = method;
    }

    /**
     * <p>
     * This method returns the request headers set by {@link #setHeaders(Map)}
     * method.</p>
     *
     * @return headers Headers in key-value pair
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * <p>
     * This method sets the request headers. </p>
     * Example
     * <pre>
     * {@code
     *
     * HttpRequestResponse hrr = new HttpRequestResponse();
     * hrr.setUrl("https://www.google.com");
     * hrr.setMethod("GET");
     *
     * Map<String, String> headers = new HashMap<String,String>();
     * headers.put("Accept","text/html");
     *
     * hrr.setHeaders(headers); // This will set the request headers.
     *
     * }
     *
     * </pre>
     *
     * @param headers Headers in key-value pair.
     */
    public void setHeaders(Map<String, String> headers) {
        System.out.println("Headers : " + headers);
        this.headers = headers;
    }

    /**
     * <p>
     * This method will return the data received from the server</p>
     *
     * @return response data received from the server
     */
    public String getResponse() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String data;
            while ((data = br.readLine()) != null) {
                response.append(data);
            }
            System.out.println(String.format("Response : %s", response.toString()));
            return response.toString();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        return null;
    }

    public String getErrorString() {
        return errorMessage.toString();
    }

    /**
     * <p>
     * This method will return the data sent to the server during "POST" call
     * set by
     * {@link com.informatica.core.HttpRequestResponse#setRequestData(String)}
     * method.</p>
     *
     * @return requestData data send to the server.
     */
    public String getRequestData() {
        return requestData;
    }

    /**
     * <p>
     * This method will copy the data to an internal buffer "requestData".</p>
     *
     * @param requestData Data/payload that will be sent to the server during
     * "POST" call.
     */
    public void setRequestData(String requestData) {
        System.out.println(String.format("RequestData : %s", requestData));
        this.requestData = requestData;
    }

    /**
     * <p>
     * This method returns the HTTP status code.</p>
     *
     * @return urlStatus HTTP status code Ex : 200, 403, 500........
     */
    public short getUrlStatus() {
        return urlStatus;
    }

    /**
     * <p>
     * This method returns the url set by
     * {@link com.informatica.core.HttpRequestResponse#setUrl(String)}
     * method.</p>
     *
     * @return url URL as String is returned if set, else null is returned.
     */
    public String getUrl() {
        return url;
    }

    /**
     * <p>
     * This method will set the URL.</p>
     *
     * @param url URL in String format should be provided.
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * <p>
     * This will create a new {@link HttpURLConnection} object and opens the
     * connection on it using new {@link URL} object.</p>
     *
     * <p>
     * The response from the server is fetched using
     * {@link HttpURLConnection#getInputStream()} method.</br>
     * and is stored in StringBuffer and later converted to string</p>
     *
     */
    public void makeCall() {
        try {
            // flush the buffer
            response.delete(0, response.length());

            System.out.println(String.format("Calling URL : %s", url));
            URL u = new URL(this.url); // Create URL object
            connection = (HttpURLConnection) u.openConnection();
            connection.setRequestMethod(method); // Set request method

            //Add headers
            for (Entry<String, String> header : headers.entrySet()) {
                connection.setRequestProperty(header.getKey(), header.getValue());
            }

            connection.setConnectTimeout(CONNECTION_TIMEOUT);

            // check whether the method is GET/POST, if POST method is used then send the data
            if (method.equalsIgnoreCase("POST")) {
                connection.setDoOutput(true);
                DataOutputStream dout = new DataOutputStream(connection.getOutputStream());
                dout.writeBytes(requestData);
                dout.flush();
                dout.close();
            }

            connection.connect(); // Makes connection to endpoint.
            urlStatus = (short) connection.getResponseCode();

            if (urlStatus != 200) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getErrorStream()))) {
                    errorMessage.delete(0, errorMessage.length());
                    String data;
                    while ((data = br.readLine()) != null) {
                        errorMessage.append(data);
                    }
                    System.out.println(errorMessage.toString());
                }

            }
            System.out.println("Call completed");

        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {

        }
    }

    public InputStream getResponseStream() {
        try {
            return connection.getInputStream();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        if (connection.getInputStream() != null) {
            connection.getInputStream().close();
        }
    }

}
