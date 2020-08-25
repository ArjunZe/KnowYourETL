/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 * https://hc.apache.org/httpcomponents-client-ga/httpclient/examples/org/apache/http/examples/client/ClientMultiThreadedExecution.java
 * https://www.mrityunjay.com/index-and-search-structured-xml-documents-using-apache-solr/
 */
package com.infa.knowyouretl;

import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author mreddy
 */
public class ClientMultiThreadedExecution {

    // static vars
    public static String serverUrl;
    public static String icSessionId;
    public static String uname;
    public static String passwd;
    public static String hostname;
    public static String fileType;
    public static String podID = null;
    private static HttpRequestResponse sHrr;
    private static Map<String, String> sHeaders = new HashMap<String, String>();
    private static Map<String, String> mctMapList = new HashMap<>();
    private static List<String> mctIdList;

    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.out.println();
            System.out.println("to run -> java -jar knowYourETL.jar <IICSusername> <IICSpassword> <hostname like dm-us.informaticacloud.com> <OutPutType as xml/json>");
            System.exit(0);
        }
        uname = args[0];
        passwd = args[1];
        hostname = args[2];
        if (args[3].equalsIgnoreCase("xml")) {
            ClientMultiThreadedExecution.fileType = "xml";
        }
        if (args[3].equalsIgnoreCase("json")) {
            ClientMultiThreadedExecution.fileType = "json";
        }
        ClientMultiThreadedExecution.mctIdList = new ArrayList();
        ClientMultiThreadedExecution.login();
        ClientMultiThreadedExecution.getMttList();
        //https://stackoverflow.com/questions/23920425/loop-arraylist-in-batches
        //final List<String> listToBatch = new ArrayList<>();
        //listToBatch.add("test");
        final List<List<String>> batch = Lists.partition(ClientMultiThreadedExecution.mctIdList, 10);
        batch.forEach((list) -> {
            // Add your code here
            // list.forEach((mcid) -> {
            //   System.out.println("calling httpts:/getmci:1111/" + mcid);
            //});
            try {
                ClientMultiThreadedExecution.devideNconcur(list);
            } catch (IOException ioe) {
                System.out.println(" - error: " + ioe);
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(ClientMultiThreadedExecution.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
        //
    }

    public static void devideNconcur(List<String> batch) throws IOException, InterruptedException {
        // Create an HttpClient with the ThreadSafeClientConnManager.
        // This connection manager must be used if more than one thread will
        // be using the HttpClient.
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        //DO NOT SET MaxTotal to more than 10.
        cm.setMaxTotal(10);
        try (CloseableHttpClient httpclient = HttpClients.custom()
                .setConnectionManager(cm)
                .build()) {
            // create an array of URIs to perform GETs on
            List<String> mctURI = new ArrayList();
            batch.forEach((mcid) -> {
                mctURI.add(ClientMultiThreadedExecution.serverUrl + "/api/v2/mttask/" + mcid);
            });
            /*String[] urisToGet = {
                ClientMultiThreadedExecution.serverUrl + "/api/v2/mttask/",
                "https://extendsclass.com/mock/rest/d2af925eee064f3518279e8b1d1ed1ce/ns",
                "https://extendsclass.com/mock/rest/d2af925eee064f3518279e8b1d1ed1ce/ns",};
             */
            // create a thread for each URI
            GetThread[] threads = new GetThread[mctURI.size()];
            for (int i = 0; i < mctURI.size(); i++) {
                HttpGet httpget = new HttpGet(mctURI.get(i));
                threads[i] = new GetThread(httpclient, httpget, i + 1);
            }
            // start the threads
            for (GetThread thread : threads) {
                thread.start();
            }
            // join the threads
            for (GetThread thread : threads) {
                thread.join();
            }
        }
    }

    /**
     * A thread that performs a GET.
     */
    static class GetThread extends Thread {

        private final CloseableHttpClient httpClient;
        private final HttpContext context;
        private final HttpGet httpget;
        private final int id;

        public GetThread(CloseableHttpClient httpClient, HttpGet httpget, int id) {
            this.httpClient = httpClient;
            this.context = new BasicHttpContext();
            this.httpget = httpget;
            this.id = id;
        }

        /**
         * Executes the GetMethod and prints some status information.
         */
        @Override
        public void run() {
            try {
                System.out.println("Thread-" + id + ": getting "+ClientMultiThreadedExecution.fileType+" using " + httpget.getURI());
                String[] urlToekns = httpget.getURI().toString().split("/");
                httpget.addHeader("Accept", "Application/"+ClientMultiThreadedExecution.fileType);
                httpget.addHeader("icsessionid", ClientMultiThreadedExecution.icSessionId);
                try (CloseableHttpResponse response = httpClient.execute(httpget, context)) {
                    System.out.println("Thread-" + id + ": received "+ClientMultiThreadedExecution.fileType+" for " + urlToekns[urlToekns.length - 1]);
                    // get the response body as an array of bytes
                    // HttpEntity entity = response.getEntity().
                    String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    System.out.println("Thread-" + id + ": writing mtt "+ClientMultiThreadedExecution.fileType+" of " + urlToekns[urlToekns.length - 1]);
                    File file = new File("./xmlStore/" + urlToekns[urlToekns.length - 1] +"."+ClientMultiThreadedExecution.fileType);
                    FileUtils.writeStringToFile(file, responseBody);
                    //https://www.tutorialspoint.com/jsoup/jsoup_parse_body.htm
                    /*if (entity != null) {
                        byte[] bytes = EntityUtils.toByteArray(entity);
                        System.out.println(id + " - " + bytes.length + " bytes read");
                        System.out.println(id + " - Response body: " + responseBody);
                    }*/
                }
            } catch (IOException e) {
                System.out.println(id + " - error: " + e);
            }
        }

    }

    public static void login() {
        String payload;
        int code;
        JSONObject obj = new JSONObject();
        obj.put("@type", "login");
        obj.put("username", uname);
        obj.put("password", passwd);
        payload = obj.toString();
        System.out.println(payload);
        String line;
        StringBuilder jsonString = new StringBuilder();
        try {
            //http://stackoverflow.com/questions/15570656/how-to-send-request-payload-to-rest-api-in-java
            URL url = new URL("https://" + hostname + "/ma/api/v2/user/login");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json;");
            try (OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream(), "UTF-8")) {
                writer.write(payload);
            }
            code = connection.getResponseCode();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getResponseCode() / 100 == 2 ? connection.getInputStream() : connection.getErrorStream()))) {
                while ((line = br.readLine()) != null) {
                    jsonString.append(line);
                }
            }
            connection.disconnect();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        String s = jsonString.toString();
        JSONObject jsonObject = new JSONObject(s);   //http://stackoverflow.com/questions/16574482/decoding-json-string-in-java
        if (code != 200) {
            String e = jsonObject.getString("@type");
            if (e.equals("error")) {
                System.out.println(jsonObject.getString("description"));
                System.exit(0);
            }
        }
        ClientMultiThreadedExecution.icSessionId = jsonObject.getString("icSessionId");
        ClientMultiThreadedExecution.serverUrl = jsonObject.getString("serverUrl");
        System.out.print("token & url " + jsonObject.getString("icSessionId") + "," + jsonObject.getString("serverUrl"));

    }

    public static void getMttList() {
        sHrr = new HttpRequestResponse();
        sHeaders.put("Accept", "application/xml");
        sHrr.setMethod("GET");
        sHeaders.put("icsessionid", ClientMultiThreadedExecution.icSessionId);
        sHrr.setHeaders(sHeaders);
        sHrr.setUrl(ClientMultiThreadedExecution.serverUrl + "/api/v2/mttask");
        sHrr.makeCall();
        if (sHrr.getUrlStatus() == 200) {
            String mtList = sHrr.getResponse();
            try {
                Document document = Jsoup.parseBodyFragment(mtList);
                Element body = document.body();
                //Elements aList = document.select("mtTask");
                Elements paragraphs = body.getElementsByTag("id");
                paragraphs.forEach((paragraph) -> {
                    ClientMultiThreadedExecution.mctIdList.add(paragraph.text());
                });
                /* logger.info(aList);
                if (aList.size() <= 0) {
                //System.out.print("emprty");
                } else {
                aList.forEach((eachAgentInfo) -> {
                mctMapList.put(eachAgentInfo.id(), eachAgentInfo.attr("href").split("'")[1]);
                });
                }*/
            } catch (JSONException e) {
                // TODO Auto-generated catch block
            }

        } else {
            //AlertWindow.error(sHrr.getErrorString());
            System.out.print(String.format("Login failed : HTTP status = %d", (int) sHrr.getUrlStatus()));
        }
    }

}
