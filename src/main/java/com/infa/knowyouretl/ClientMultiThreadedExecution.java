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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author mallikarjun reddy
 */
public class ClientMultiThreadedExecution {

    // static vars
    public static String serverUrl;
    public static String icSessionId;
    public static String uname;
    public static String passwd;
    public static String hostname;
    public static String contentType;
    public static String podID = null;
    private static HttpRequestResponse sHrr;
    private static Map<String, String> sHeaders = new HashMap<String, String>();
    private static List<String> idList;
    private static List<String> operations;
    private static String mode;
    private static String lastruntime;

    public static void main(String[] args) throws IOException, InterruptedException {
        int argLength = args.length;
        if (argLength < 4) {
            System.out.println("to run -> java -jar knowYourETL.jar <IICSusername> <IICSpassword> <hostname like dm-us.informaticacloud.com> <colon Separated AssetTypes connection:mttask:dsstask:drstask > <OutPutType as xml/json> <delta>");
            System.exit(0);
        }
        if (argLength == 6) {
            mode = args[5];
            if (Files.exists(Paths.get(".lastruntime"))) {
                lastruntime = new String(Files.readAllBytes(Paths.get(".lastruntime")));
                System.out.println("Running in Delta Mode and Last Run Time is '" + lastruntime + "'");
            } else {
                System.out.println("Delta Mode Selected but .lastrumtime file does not exists.\nNote: .lastruntime is created at the end of the tool execution.\nIf this is the 1st time, try without delta. Otherwise, create .lastruntime with UTC time in it. Ex: 2021-05-30T14:19:38.146Z");
                System.exit(0);
            }
        }
        uname = args[0];
        passwd = args[1];
        hostname = args[2];
        if (args[4].equalsIgnoreCase("xml")) {
            ClientMultiThreadedExecution.contentType = "xml";
        }
        if (args[4].equalsIgnoreCase("json")) {
            ClientMultiThreadedExecution.contentType = "json";
        }

        ClientMultiThreadedExecution.operations = new ArrayList();
        List<String> assetTypes = Arrays.asList(args[3].split(":"));
        assetTypes.forEach((asset) -> {
            System.out.println("Adding asset type '" + asset + "' to the operations list");
            ClientMultiThreadedExecution.operations.add(asset);
        });
        System.out.println("Content-Type is set to '" + args[4] + "'");
//        ClientMultiThreadedExecution.operations.add("dsstask");
//        ClientMultiThreadedExecution.operations.add("drstask");
//        ClientMultiThreadedExecution.operations.add("mttask");
//        ClientMultiThreadedExecution.operations.add("connection");
        ClientMultiThreadedExecution.idList = new ArrayList();
        //ClientMultiThreadedExecution.login();
        //https://stackoverflow.com/questions/426758/running-a-java-thread-in-intervals
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        Runnable periodicTask = new Runnable() {
            public void run() {
                ClientMultiThreadedExecution.login();
            }
        };
        executor.scheduleAtFixedRate(periodicTask, 0, 25 * 60, TimeUnit.SECONDS);
        Thread.sleep(10000);
        operations.forEach((operation) -> {
            try {
                //https://stackoverflow.com/questions/23920425/loop-arraylist-in-batches
                ClientMultiThreadedExecution.getIdList(operation);
            } catch (ParseException ex) {
                Logger.getLogger(ClientMultiThreadedExecution.class.getName()).log(Level.SEVERE, null, ex);
            }
            final List<List<String>> batch = Lists.partition(ClientMultiThreadedExecution.idList, 10);
            batch.forEach((list) -> {
                try {
                    ClientMultiThreadedExecution.devideNconcur(list, operation);
                } catch (IOException ioe) {
                    System.out.println(" - error: " + ioe);
                } catch (InterruptedException ex) {
                    java.util.logging.Logger.getLogger(ClientMultiThreadedExecution.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
        });

        Instant instant = Instant.now();
        System.out.println("Writing Last Run Time " + instant + " to .lastruntime file");
        FileWriter myWriter = new FileWriter(".lastruntime");
        myWriter.write(instant.toString());
        myWriter.close();
        System.out.println("Done!");
        System.exit(0);
    }

    public static void devideNconcur(List<String> batch, String operation) throws IOException, InterruptedException {
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
            batch.forEach((id) -> {
                mctURI.add(ClientMultiThreadedExecution.serverUrl + "/api/v2/" + operation + "/" + id);
            });
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
                System.out.println("Thread-" + id + ": getting " + ClientMultiThreadedExecution.contentType + " using " + httpget.getURI());
                String[] urlToekns = httpget.getURI().toString().split("/");
                httpget.addHeader("Accept", "Application/" + ClientMultiThreadedExecution.contentType);
                httpget.addHeader("icsessionid", ClientMultiThreadedExecution.icSessionId);
                try (CloseableHttpResponse response = httpClient.execute(httpget, context)) {
                    System.out.println("Thread-" + id + ": received " + ClientMultiThreadedExecution.contentType + " for " + urlToekns[urlToekns.length - 1]);
                    // get the response body as an array of bytes
                    // HttpEntity entity = response.getEntity().
                    String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    System.out.println("Thread-" + id + ": writing " + ClientMultiThreadedExecution.contentType + " of " + urlToekns[urlToekns.length - 1]);
                    File file = new File("./xmlStore/" + urlToekns[urlToekns.length - 2] + "/" + urlToekns[urlToekns.length - 1] + "." + ClientMultiThreadedExecution.contentType);
                    FileUtils.writeStringToFile(file, responseBody);
                    //https://www.tutorialspoint.com/jsoup/jsoup_parse_body.htm
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
        System.out.println("Login Payload :" + payload);
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
        System.out.print("icSessionID & Pod URL is -> " + jsonObject.getString("icSessionId") + "," + jsonObject.getString("serverUrl"));

    }

    public static void getIdList(String operation) throws ParseException {
        sHrr = new HttpRequestResponse();
        sHeaders.put("Accept", "application/xml");
        sHrr.setMethod("GET");
        sHeaders.put("icsessionid", ClientMultiThreadedExecution.icSessionId);
        sHrr.setHeaders(sHeaders);
        sHrr.setUrl(ClientMultiThreadedExecution.serverUrl + "/api/v2/" + operation);
        sHrr.makeCall();
        if (sHrr.getUrlStatus() == 200) {
            String mtList = sHrr.getResponse();
            try {
                Document document = Jsoup.parseBodyFragment(mtList);
                Element body = document.body();
                //Elements aList = document.select("mtTask");
                Elements elements = document.body().select(operation);
//                for (Element element : elements) {
//                    ClientMultiThreadedExecution.idList.add(element.select("id").text());
//                    System.out.println(element.select("id").text() + "-" + element.select("updateTime").text());
//                }
//                Elements paragraphs = body.getElementsByTag("id");
//                String updateTime = body.getElementsByTag("updateTime").text();
                if (mode != null && mode.equalsIgnoreCase("delta")) {
                    
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'");
                    df.setTimeZone(TimeZone.getTimeZone("UTC"));
                    DateTime lastruntimeUTC = ISODateTimeFormat.dateTimeParser().parseDateTime(lastruntime);
                    DateTime updateTimeUTC;
                    for (Element element : elements) {
                        updateTimeUTC =ISODateTimeFormat.dateTimeParser().parseDateTime(element.select("updateTime").text());
                        System.out.println("after ? "+updateTimeUTC.isAfter(lastruntimeUTC));
                        System.out.println(updateTimeUTC+"-"+lastruntimeUTC);
                        if (updateTimeUTC.isAfter(lastruntimeUTC)) {
                            ClientMultiThreadedExecution.idList.add(element.select("id").text());         
                        System.out.println(element.select("id").text() + "-" + element.select("updateTime").text()+"-"+lastruntime);
                        }
                    }
                } else {
                    for (Element element : elements) {
                        ClientMultiThreadedExecution.idList.add(element.select("id").text());
                        //System.out.println(element.select("id").text() + "-" + element.select("updateTime").text());
                    }
                }
//                System.out.println("Adding updateTime " + body.getElementsByTag("updateTime").text());
//                paragraphs.forEach((paragraph) -> {
//                    ClientMultiThreadedExecution.idList.add(paragraph.text());
//                });
            } catch (JSONException e) {
                e.printStackTrace();
            }

        } else {
            System.out.print(String.format("Login failed : HTTP status = %d", (int) sHrr.getUrlStatus()));
        }
    }

}
