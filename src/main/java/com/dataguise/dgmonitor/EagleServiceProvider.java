package com.dataguise.dgmonitor;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.*;

public class EagleServiceProvider {

    /**IP address of eagle service host*/
    private  String host ="192.168.6.148";
    /**port number of eagle service*/
    private  String port = "9099";
    /**username of eagle service*/
    private  String username = "admin";
    /**password of of eagle service*/
    private  String password = "secret";

    public EagleServiceProvider(){}

//    public EagleServiceProvider() throws FileNotFoundException {
////        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
//
////        URL url = classLoader.getResource("/src/main/java/com/dataguise/dgmonitor/dgMonitorConfig.properties");
//        FileInputStream in =new FileInputStream("com\\dataguise\\dgmonitor\\dgMonitorConfig.Properties");
//        try{
//            Properties properties = new Properties();
//            properties.load(in);
//
//            host = properties.getProperty("host");
//            port = properties.getProperty("port");
//            username = properties.getProperty("username");
//            password = properties.getProperty("password");
//
//            properties.clear();
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }
//        finally{
//            try {
//                if(in!=null)
//                    in.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }

//    public EagleServiceProvider() throws Exception {
//        //Read properties files, initialize Eagle credentials
//        String configPath = "src\\main\\java\\com\\dataguise\\dgmonitor\\dgMonitorConfig.properties";
//        Properties prop = new Properties();
//        try{
//            FileInputStream inputStream = new FileInputStream(configPath);
//            prop.load(inputStream);
//            host = prop.getProperty("host");
//            port = prop.getProperty("port");
//            username = prop.getProperty("username");
//            password = prop.getProperty("password");
//        }catch (FileNotFoundException e){
//            throw new FileNotFoundException("void setProperties(String propertyFileFullPath): property file '"+ configPath + "' not found in the classpath");
//        }catch (Exception e){
//            throw new Exception("check the seeting in property file: "+ configPath);
//        }
//    }

    public EagleServiceProvider(String host, String port, String username, String password){
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public String getHost(){
        return this.host;
    }

    public String getPort(){
        return this.port;
    }

    public String getUsername(){
        return this.username;
    }

    public String getPassword(){
        return this.password;
    }
    /**
     * Send or update multiple Hdfspolicies to Eagle
     * @param policySet Policies collections
     * @return  JSONObject, which is a HTTP Response
     * */
    public JSONObject sendHDFSPolicyBatch(Set<HDFSPolicy> policySet) throws Exception{
        JSONObject result = null;
        StringBuilder jsonData = new StringBuilder(800);
        if(0 == policySet.size()) return result;


        try {
            jsonData.append("[");
            Iterator<HDFSPolicy> iterator = policySet.iterator();
            while (iterator.hasNext()) {
                HDFSPolicy hdfsPolicy = iterator.next();
                jsonData.append(hdfsPolicy.getJSONFormat());
                if (iterator.hasNext()) jsonData.append(",");
            }
            jsonData.append("]");

            String restUrl = "http://" + host + ":" + port + DgMonitorConstants.URL_OF_RESTAPI+"?serviceName="+DgMonitorConstants.ALERT_DEFINITION_SERVICE;
            HttpPost httpPost = createPost(restUrl, jsonData.toString());
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpResponse response = httpClient.execute(httpPost);
            result = responseParser(response);
            httpPost.releaseConnection();
        }catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
            throw e;
        }catch (JSONException e){
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
        return result;
    }



    /**
     * Send/Update a single HDFS policy
     * @param hdfsPolicy HDFSPolicy
     * @return  Returns a JSONObject, which is a HTTP Response
     * */
    public JSONObject sendHDFSPolicy(HDFSPolicy hdfsPolicy){
        JSONObject result = null;
        Set<HDFSPolicy> set = new HashSet<HDFSPolicy>();
        set.add(hdfsPolicy);
        try {
            result = sendHDFSPolicyBatch(set);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }



    /**
     * Get policy details
     * @return JSONObject , contains multiple policy definitions
     * @param policyQueryParams Represent one or multiple policies
     * */
    public JSONObject getHDFSPolicy(PolicyQueryParams policyQueryParams, int pageSize) throws UnsupportedEncodingException {
        JSONObject jsonObject =null;
        String restUrl = "http://" + host + ":" + port + DgMonitorConstants.URL_OF_RESTAPI+"?query="+DgMonitorConstants.ALERT_DEFINITION_SERVICE + "["+ URLEncoder.encode(policyQueryParams.getParams(),"UTF-8") + "]";
        restUrl = restUrl+URLEncoder.encode("{*}","UTF-8")+"&pageSize="+pageSize;
        HttpGet httpGet = createGet(restUrl);
        HttpClient httpClient = HttpClientBuilder.create().build();
        try {
            HttpResponse response = httpClient.execute(httpGet);
            jsonObject = responseParser(response);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        } finally {
            httpGet.releaseConnection();
        }
        return jsonObject;

    }

    public JSONObject getHDFSPolicy(PolicyQueryParams policyQueryParams) throws UnsupportedEncodingException {
        return getHDFSPolicy(policyQueryParams, 100);
    }

    /**
     * Response parser, convert response to a json object
     * @return  JSONObject
     * */
    JSONObject responseParser(HttpResponse httpResponse) throws IOException, JSONException {
        BufferedReader rd = new BufferedReader(
                new InputStreamReader(httpResponse.getEntity().getContent()));

        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        JSONObject jsonObject = new JSONObject(result.toString());
        return jsonObject;
    }

    /**
     * Delete an Eagle HDFS policy
     * @param policyQueryParams the id of the policy
     * @return  JSONObject, http response
     * */
    public JSONObject deleteHDFSPolicy(PolicyQueryParams policyQueryParams) throws IOException, JSONException {
        JSONObject result = null;
        try {
            ArrayList<String> encodedRowkeys = getEncodedRowkeys(policyQueryParams);
            if(encodedRowkeys.size()==0) return result;
            //crate url and post send
            String  restUrl = "http://" + host + ":" + port + DgMonitorConstants.URL_OF_RESTAPI + "/delete?serviceName=" + DgMonitorConstants.ALERT_DEFINITION_SERVICE + "&byId=true";
            for(int i = 0; i < encodedRowkeys.size(); i++){
                String jsonData = "[\"" + encodedRowkeys.get(i) + "\"]";
                HttpPost httpPost = createPost(restUrl,jsonData);
                HttpClient httpClient = HttpClientBuilder.create().build();
                HttpResponse httpResponse= httpClient.execute(httpPost);
                result = responseParser(httpResponse);
                httpPost.releaseConnection();
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw e;
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
        return result;
    }

    /**
     * Get all encodedRowKey for selected policies
     * @param policyQueryParams Represent
     * */
    public ArrayList<String> getEncodedRowkeys(PolicyQueryParams policyQueryParams) throws UnsupportedEncodingException, JSONException {
        ArrayList<String> encodedRowkeys = new ArrayList<String>();
        JSONObject jsonObject = null;
        try {
            jsonObject = getHDFSPolicy(policyQueryParams);
            JSONArray jsonArray = (JSONArray) jsonObject.get("obj");
            for(int i = 0; i < jsonArray.length(); i++){
                JSONObject data = (JSONObject) jsonArray.get(i);
                encodedRowkeys.add(data.getString("encodedRowkey"));
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw e;
        } catch (JSONException e) {
            throw e;
        }
        return encodedRowkeys;
    }

    /**
     * mark folder/file with sensitivity type
     * @param site site name
     * @param fileSensitivity key is filePath, value is a set which contains all file sensitivity type
     * */
    public JSONObject setSensitivity(String site, Map<String,Set<String>> fileSensitivity) throws Exception {
        JSONObject result = null;
        String restUrl="http://"+ host +":"+port + DgMonitorConstants.URL_OF_RESTAPI +"?serviceName="+DgMonitorConstants.FILE_SENSITIVITY_SERVICE;
        String sensitiveData = "";
        //construct json data mask
        StringBuilder sb = new StringBuilder(100);
        sb.append("[");
        Iterator iterator = fileSensitivity.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry pair = (Map.Entry) iterator.next();
            //construct a json data
            String fPath = (String) pair.getKey();
            Set<String> fType = (Set<String>) pair.getValue();
            String tag = fTypeToTag(fType);
            sb.append("{\"tags\":{\"site\" : \"" + site + "\",\"filedir\" : \"" + fPath + "\"},\"sensitivityType\": \"" + tag + "\"}");
            if(iterator.hasNext()) sb.append(",");
        }
        sb.append("]");

        try{
            HttpPost httpPost = createPost(restUrl,sb.toString());
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpResponse response = httpClient.execute(httpPost);
            result = responseParser(response);
            httpPost.releaseConnection();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return result;
    }

    /**
     * set single file with its sensitivity
     * @param site siteName
     * @param fPath Full path of the file/folder
     * @param fTypes contains all sensitivity types of this file/folder
     * */
    public JSONObject setSensitivity(String site, String fPath, Set<String> fTypes) throws Exception {
        JSONObject result = null;
        Map<String,Set<String>> map = new HashMap<String, Set<String>>();
        map.put(fPath,fTypes);
        result = setSensitivity(site,map);
        return result;
    }

    /**
     * @params fTypes contains all the sensitive types
     * @return concatenate the types in the form: "type1|type2|type3"
     * */
    public String fTypeToTag(Set<String> fTypes){
        StringBuilder tag = new StringBuilder();
        Iterator iterator = fTypes.iterator();
        if(iterator.hasNext()){
            tag.append(iterator.next());
        }
        while(iterator.hasNext()){
            tag.append('|').append(iterator.next());
        }
        return tag.toString();
    }


    /**
     * delete folder/file with sensitivity type
     * @param site site name
     * @param fPath file/folder path
     * */

    //remove single file's tag
    public JSONObject removeSensitivity(String site,String fPath) throws Exception {
        JSONObject result;
        Set<String> set = new HashSet<String>();
        set.add(fPath);
        result = removeSensitivity(site,set);
        return result;
    }
    // remove multiple file's tag
    public JSONObject removeSensitivity(String site, Set<String> fPaths) throws Exception {
        JSONObject result;
        Map<String, Set<String>> map = new HashMap<String, Set<String>>();
        HashSet<String> empty = new HashSet<String>();
        for(String fPath:fPaths){
            map.put(fPath,empty);
        }
        result = setSensitivity(site,map);
        return result;
    }

    /**
     * Get sensitivity of a folder/file
     * @param  site site name
     * @param  fpath full path of file/folder, eg: /folder
     * */
    public String getSensitivityTag(String site, String fpath) throws UnsupportedEncodingException {
        String tag = null;
        String restUrl = "http://"+ host +":"+port+ DgMonitorConstants.URL_OF_RESTAPI + "?query=" + DgMonitorConstants.FILE_SENSITIVITY_SERVICE
                + "["
                + URLEncoder.encode("@site=" + "\""+site + "\"" +" and @filedir=" +  "\"" + fpath +  "\"" ,"UTF-8")
                + "]";
        restUrl = restUrl+URLEncoder.encode("{*}","UTF-8")+"&pageSize=100";
        HttpGet httpGet = createGet(restUrl);
        HttpClient httpClient = HttpClientBuilder.create().build();
        try {
            HttpResponse response = httpClient.execute(httpGet);
            JSONObject jsonObject = responseParser(response);
            JSONArray jsonArray = jsonObject.getJSONArray("obj");
            JSONObject fdata = jsonArray.getJSONObject(0);
            tag = fdata.getString("sensitivityType");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        } finally {
            httpGet.releaseConnection();
        }
        return tag;
    }


    /**
     * Get sensitivity meta data of a site
     * @param  site site name
     * */
    public Map<String, String> getAllSensitivityTag(String site) throws UnsupportedEncodingException,IOException,JSONException {
        Map<String, String> map = new HashMap<String, String>();
        String restUrl = "http://" + host + ":" + port + DgMonitorConstants.URL_OF_RESTAPI + "?query=" + DgMonitorConstants.FILE_SENSITIVITY_SERVICE + URLEncoder.encode("[@site=" + "\"" + site + "\"" + "]","UTF-8");
        restUrl = restUrl + URLEncoder.encode("{*}","UTF-8") + "&pageSize=100";
        HttpGet httpGet = createGet(restUrl);
        HttpClient httpClient = HttpClientBuilder.create().build();
        try {
            HttpResponse response = httpClient.execute(httpGet);
            JSONObject jsonObject = responseParser(response);
            JSONArray jsonArray = jsonObject.getJSONArray("obj");
            for ( int i = 0; i < jsonArray.length(); i++){
                JSONObject fdata = jsonArray.getJSONObject(i);
                String filedir = fdata.getJSONObject("tags").getString("filedir");
                String fType = fdata.getString("sensitivityType");
                map.put(filedir,fType);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } catch (JSONException e) {
            e.printStackTrace();
            throw e;
        } finally {
            httpGet.releaseConnection();
        }
        return map;
    }


    /**
     * This method is used to create HttpPost with authentication header
     * @param restUrl restURl of Eagle Service
     * @return return HttpPost request based on authentication
     * * */
    public HttpPost createPost(String restUrl,String jsonData) throws UnsupportedEncodingException {
        HttpPost post;
        post = new HttpPost(restUrl);
        String auth=new StringBuffer(username).append(":").append(password).toString();
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);
        post.setHeader("AUTHORIZATION", authHeader);
        post.setHeader("Content-Type", "application/json");
        post.setEntity(new StringEntity(jsonData));
        return post;
    }

    /**
     * This method is used to create HttpGet with authentication header
     * @param restUrl restURl of Eagle Service
     * @return return HttpGet request based on authentication
     * * */
    public HttpGet createGet(String restUrl){
        HttpGet get;
        get = new HttpGet(restUrl);
        String auth=new StringBuffer(username).append(":").append(password).toString();
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);
        get.setHeader("AUTHORIZATION", authHeader);
        get.setHeader("Content-Type", "application/json");
        return get;
    }


}
