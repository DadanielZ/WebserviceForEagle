package com.dataguise.dgmonitor;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Created by dataguise on 4/14/2016.
 */
public class ServiceTest {
    @Test
    public void testEagleServiceConstructor() throws Exception {
        EagleServiceProvider eagle = new EagleServiceProvider();
        Assert.assertEquals(eagle.getHost(),"192.168.6.148");
        Assert.assertEquals(eagle.getPort(),"9099");
        Assert.assertEquals(eagle.getUsername(),"admin");
        Assert.assertEquals(eagle.getPassword(),"secret");
    }

    @Test
    public void testStrToJsonArray() throws Exception {
        String jsondata = "[{\n" +
                " \"tags\": {\n" +
                "  \"site\": \"sandbox\",\n" +
                "  \"application\": \"hdfsAuditLog\",\n" +
                "  \"policyId\": \"testPolicy\",\n" +
                "  \"alertExecutorId\": \"hdfsAuditLogAlertExecutor\",\n" +
                "  \"policyType\": \"siddhiCEPEngine\"\n" +
                " },\n" +
                " \"desc\": \"test alert policy,second time\",\n" +
                " \"policyDef\": \"{\\\"type\\\": \\\"siddhiCEPEngine\\\", \\\"expression\\\": \\\"from hdfsAuditLogEventStream[src ==â€™/test0â€™] select * insert into outputStream;\\\"}\",\n" +
                " \"notificationDef\": \"[{ \\\"sender \\\":  \\\"noreply-eagle@company.com \\\", \\\"recipients \\\": \\\" user@company.com \\\", \\\" subject\\\": \\\" test alert policy \\\", \\\"flavor \\\": \\\" email \\\", \\\"id \\\": \\\"email_1 \\\"}]\",\n" +
                " \"enabled\": true\n" +
                "}]\n";
        JSONArray jsonArray = Utilities.strToJsonArray(jsondata);
        int size = jsonArray.length();
        for(int i=0;i<size;i++){
            System.out.println(jsonArray.get(i));
        }
        JSONObject jsonObject = (JSONObject) jsonArray.get(0);
        System.out.println(jsonObject.get("tags"));
        System.out.println(jsonObject.get("policyDef"));

    }

    @Test
    public void testStrToJson() throws JSONException {
        String json = "{\"meta\":{\"elapsedms\":7,\"totalResults\":5,\"lastTimestamp\":0,\"firstTimestamp\":0},\"success\":true,\"obj\":[{\"prefix\":\"alertdef\",\"tags\":{\"site\":\"sandbox\",\"application\":\"userProfile\",\"policyId\":\"userProfile\",\"alertExecutorId\":\"userProfileAnomalyDetectionExecutor\",\"policyType\":\"MachineLearning\"},\"encodedRowkey\":\"YEktKX_____62aP_6x97yg7krzMANd9Hby--xyCZKe3bkGteXKQFUNuQa15e-7-M4umcZQ\",\"description\":\"user profile anomaly detection\",\"policyDef\":\"{\\\"type\\\":\\\"MachineLearning\\\",\\\"alertContext\\\":{\\\"site\\\":\\\"sandbox\\\",\\\"application\\\":\\\"userProfile\\\",\\\"component\\\":\\\"testComponent\\\",\\\"description\\\":\\\"ML based user profile anomaly detection\\\",\\\"severity\\\":\\\"WARNING\\\",\\\"notificationByEmail\\\":\\\"true\\\"},\\\"algorithms\\\":[{\\\"name\\\":\\\"EigenDecomposition\\\",\\\"evaluator\\\":\\\"org.apache.eagle.security.userprofile.impl.UserProfileAnomalyEigenEvaluator\\\",\\\"description\\\":\\\"EigenBasedAnomalyDetection\\\",\\\"features\\\":\\\"getfileinfo, open, listStatus, setTimes, setPermission, rename, mkdirs, create, setReplication, contentSummary, delete, setOwner, fsck\\\"},{\\\"name\\\":\\\"KDE\\\",\\\"evaluator\\\":\\\"org.apache.eagle.security.userprofile.impl.UserProfileAnomalyKDEEvaluator\\\",\\\"description\\\":\\\"DensityBasedAnomalyDetection\\\",\\\"features\\\":\\\"getfileinfo, open, listStatus, setTimes, setPermission, rename, mkdirs, create, setReplication, contentSummary, delete, setOwner, fsck\\\"}]}\",\"dedupeDef\":\"{\\\"alertDedupIntervalMin\\\":\\\"0\\\",\\\"emailDedupIntervalMin\\\":\\\"0\\\"}\",\"notificationDef\":\"\",\"remediationDef\":\"\",\"enabled\":true,\"lastModifiedDate\":0,\"severity\":0,\"createdTime\":0,\"markdownEnabled\":false},{\"prefix\":\"alertdef\",\"tags\":{\"site\":\"sandbox\",\"application\":\"hdfsAuditLog\",\"policyId\":\"test2\",\"alertExecutorId\":\"hdfsAuditLogAlertExecutor\",\"policyType\":\"siddhiCEPEngine\"},\"encodedRowkey\":\"YEktKX_____62aP_6x97yoSv3B0ANd9Hby--xyCZKe0Gkk3gXKQFUHeJk1Je-7-Mrq0lGQ\",\"description\":\"\",\"policyDef\":\"{\\\"expression\\\":\\\"from hdfsAuditLogEventStream[(dst == '/alertStreamSchema')] select * insert into outputStream;\\\",\\\"type\\\":\\\"siddhiCEPEngine\\\"}\",\"dedupeDef\":\"{\\\"alertDedupIntervalMin\\\":10,\\\"emailDedupIntervalMin\\\":10}\",\"notificationDef\":\"[]\",\"remediationDef\":\"\",\"enabled\":true,\"owner\":\"admin\",\"lastModifiedDate\":1460590188795,\"severity\":0,\"createdTime\":1460500242965,\"markdownEnabled\":false},{\"prefix\":\"alertdef\",\"tags\":{\"site\":\"sandbox\",\"application\":null,\"policyId\":\"testPolicy\",\"alertExecutorId\":\"hdfsAuditLogAlertExecutor\",\"policyType\":\"siddhiCEPEngine\"},\"encodedRowkey\":\"YEktKX_____62aP_6x97yoSv3B0ANd9Hby--xyCZKe1hk6BkXvu_jK6tJRk\",\"policyDef\":\"{\\\"type\\\": \\\"siddhiCEPEngine\\\", \\\"expression\\\": \\\"from hdfsAuditLogEventStream[src ==â€™/test0â€™] select * insert into outputStream;\\\"}\",\"notificationDef\":\"[{ \\\"sender \\\":  \\\"noreply-eagle@company.com \\\", \\\"recipients \\\": \\\" user@company.com \\\", \\\" subject\\\": \\\" test alert policy \\\", \\\"flavor \\\": \\\" email \\\", \\\"id \\\": \\\"email_1 \\\"}]\",\"enabled\":true,\"lastModifiedDate\":0,\"severity\":0,\"createdTime\":0,\"markdownEnabled\":false},{\"prefix\":\"alertdef\",\"tags\":{\"site\":\"sandbox\",\"application\":\"hdfsAuditLog\",\"policyId\":\"testPolicy\",\"alertExecutorId\":\"hdfsAuditLogAlertExecutor\",\"policyType\":\"siddhiCEPEngine\"},\"encodedRowkey\":\"YEktKX_____62aP_6x97yoSv3B0ANd9Hby--xyCZKe1hk6BkXKQFUHeJk1Je-7-Mrq0lGQ\",\"policyDef\":\"{\\\"type\\\": \\\"siddhiCEPEngine\\\", \\\"expression\\\": \\\"from hdfsAuditLogEventStream[src ==â€™/test0â€™] select * insert into outputStream;\\\"}\",\"notificationDef\":\"[{ \\\"sender \\\":  \\\"noreply-eagle@company.com \\\", \\\"recipients \\\": \\\" user@company.com \\\", \\\" subject\\\": \\\" test alert policy \\\", \\\"flavor \\\": \\\" email \\\", \\\"id \\\": \\\"email_1 \\\"}]\",\"enabled\":true,\"lastModifiedDate\":0,\"severity\":0,\"createdTime\":0,\"markdownEnabled\":false},{\"prefix\":\"alertdef\",\"tags\":{\"site\":\"sandbox\",\"application\":\"hdfsAuditLog\",\"policyId\":\"test1\",\"alertExecutorId\":\"hdfsAuditLogAlertExecutor\",\"policyType\":\"siddhiCEPEngine\"},\"encodedRowkey\":\"YEktKX_____62aP_6x97yoSv3B0ANd9Hby--xyCZKe0Gkk3fXKQFUHeJk1Je-7-Mrq0lGQ\",\"description\":\"\",\"policyDef\":\"{\\\"expression\\\":\\\"from hdfsAuditLogEventStream[(cmd == 'CHGRP')] select * insert into outputStream;\\\",\\\"type\\\":\\\"siddhiCEPEngine\\\"}\",\"dedupeDef\":\"{\\\"alertDedupIntervalMin\\\":10,\\\"emailDedupIntervalMin\\\":10}\",\"notificationDef\":\"[]\",\"remediationDef\":\"\",\"enabled\":true,\"owner\":\"admin\",\"lastModifiedDate\":1460744213101,\"severity\":0,\"createdTime\":1460744213101,\"markdownEnabled\":false}],\"type\":\"org.apache.eagle.alert.entity.AlertDefinitionAPIEntity\"}\n";
        JSONObject jsonObject = new JSONObject(json);
        System.out.println(jsonObject.toString());
    }

    @Test
    public void policyQueryParamTest(){
        PolicyQueryParams policyQueryParams = new PolicyQueryParams();
        policyQueryParams.setApplication("hdfsAuditLog");
        System.out.println(policyQueryParams.getParams());
        policyQueryParams.setSite("sandbox");
        System.out.println(policyQueryParams.getParams());
        policyQueryParams.setPolicyId("test");
        System.out.println(policyQueryParams.getParams());
    }

    @Test
    public void sendHDFSPolicyBatchTest() throws Exception {
        HDFSPolicy p1 = new HDFSPolicy();
        HashSet<String> cmdSet = new HashSet<String>();
        cmdSet.add("delete");cmdSet.add("copy");
        p1.setCmd(cmdSet);
        p1.setPolicyId("policyTest1");
        p1.setSite("sandbox");


        HDFSPolicy p2 = new HDFSPolicy();
        HashSet<String> srcSet = new HashSet<String>();
        srcSet.add("/src1");srcSet.add("src2");
        p2.setSrc(cmdSet);
        p2.setPolicyId("policyTest2");
        p2.setSite("sandbox");

        HashSet<HDFSPolicy> set = new HashSet<HDFSPolicy>();
        set.add(p1);
        set.add(p2);
        EagleServiceProvider eagleServiceProvider = new EagleServiceProvider();
        eagleServiceProvider.sendHDFSPolicyBatch(set);
    }

    @Test
    public  void sendHDFSPolicyTest() throws Exception {
        HDFSPolicy p1 = new HDFSPolicy();
        HashSet<String> cmdSet = new HashSet<String>();
        cmdSet.add("add");cmdSet.add("copy");
        p1.setCmd(cmdSet);
        p1.setPolicyId("policyTest3");
        p1.setSite("sandbox");
        EagleServiceProvider eagleServiceProvider = new EagleServiceProvider();
        eagleServiceProvider.sendHDFSPolicy(p1);
//        eagleServiceProvider.sendpolicy(p1);
    }

    @Test
    public void getPolicyTest() throws Exception{
        EagleServiceProvider eagleServiceProvider = new EagleServiceProvider();
        PolicyQueryParams policyQueryParams = new PolicyQueryParams();
        policyQueryParams.setSite("sandbox");
        try {
            System.out.println(eagleServiceProvider.getHDFSPolicy(policyQueryParams));
            policyQueryParams.setApplication("hdfsAuditLog");
            System.out.println(eagleServiceProvider.getHDFSPolicy(policyQueryParams));
            policyQueryParams.setPolicyId("testAllParams");
            System.out.println(eagleServiceProvider.getHDFSPolicy(policyQueryParams));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getEncodedRowkeysTest() throws Exception {
        EagleServiceProvider eagleServiceProvider = new EagleServiceProvider();
        PolicyQueryParams policyQueryParams = new PolicyQueryParams();
        policyQueryParams.setSite("sandbox");
        policyQueryParams.setApplication("hdfsAuditLog");
        policyQueryParams.setPolicyId("test1");
        ArrayList<String> encodedRowkeys = eagleServiceProvider.getEncodedRowkeys(policyQueryParams);
        Assert.assertEquals("YEktKX_____62aP_6x97yoSv3B0ANd9Hby--xyCZKe0Gkk3fXKQFUHeJk1Je-7-Mrq0lGQ",encodedRowkeys.get(0));
    }

    @Test
    public void deletePolicyTest() throws Exception {
        HDFSPolicy hdfsPolicy = new HDFSPolicy();
        hdfsPolicy.setPolicyId("fortest");

        EagleServiceProvider eagleServiceProvider = new EagleServiceProvider();
        PolicyQueryParams policyQueryParams = new PolicyQueryParams();
        policyQueryParams.setSite("sandbox");
        policyQueryParams.setApplication("hdfsAuditLog");
        policyQueryParams.setPolicyId("tempTest");
        eagleServiceProvider.deleteHDFSPolicy(policyQueryParams);
    }

    @Test
    public void fTypeToTagTest() throws Exception{
        EagleServiceProvider eagleServiceProvider = new EagleServiceProvider();
        HashSet<String> set = new HashSet<String>();
        set.add("type1");
        set.add("type2");
        set.add("type3");
        System.out.print(eagleServiceProvider.fTypeToTag(set));
    }

    @Test
    public void setSensitivityTest() throws Exception {
        HashMap<String, Set<String>> map = new HashMap<String, Set<String>>();
        String f1 = "/test1";
        HashSet<String> hashSet1 = new HashSet<String>();
        hashSet1.add("t1");hashSet1.add("t2");hashSet1.add("t3");
        map.put(f1,hashSet1);
        String f2 = "/test0";
        HashSet<String> hashSet2 = new HashSet<String>();
        hashSet2.add("t4");hashSet2.add("t5");hashSet2.add("t6");
        map.put(f2,hashSet2);
        EagleServiceProvider eagleServiceProvider = new EagleServiceProvider();
        eagleServiceProvider.setSensitivity("sandbox",map);

        String f4 = "/test4";
        HashSet<String> hashSet4 = new HashSet<String>();
        hashSet4.add("t1");hashSet4.add("t2");hashSet4.add("t3");
        eagleServiceProvider.setSensitivity("sandbox",f4,hashSet4);
    }

    @Test
    public void removeSensitivityTest() throws Exception {
        HashSet<String> hashSet1 = new HashSet<String>();
        hashSet1.add("/test01");hashSet1.add("/test02");
        EagleServiceProvider eagleServiceProvider = new EagleServiceProvider();
        eagleServiceProvider.removeSensitivity("sandbox",hashSet1);
        eagleServiceProvider.removeSensitivity("sandbox","/user");
    }

    @Test
    public void getSensitivityTagTest() throws Exception {
        EagleServiceProvider eagleServiceProvider = new EagleServiceProvider();
        String tag = eagleServiceProvider.getSensitivityTag("sandbox","/test1");
        Assert.assertEquals("t3|t2|t1",tag);
    }

    @Test
    public void getAllFileSensitivityTest() throws Exception {
        EagleServiceProvider eagleServiceProvider = new EagleServiceProvider();
        Map<String,String> map = new HashMap<String,String>();
        map = eagleServiceProvider.getAllSensitivityTag("sandbox");
    }

    @Test
    public void Testload() {
       ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

//        URL url = classLoader.getResource("/src/main/java/com/dataguise/dgmonitor/dgMonitorConfig.properties");
        InputStream in = classLoader.getResourceAsStream("dgMonitorConfig.properties");
        try{
            Properties properties = new Properties();
            properties.load(in);

            String host = properties.getProperty("host");
            String port = properties.getProperty("port");
            String username = properties.getProperty("username");
            String password= properties.getProperty("password");

            properties.clear();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally{
            try {
                if(in!=null)
                    in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void  testSend() {
        String restUrl = "http://" + "192.168.40.131" + ":" + "9099" + DgMonitorConstants.URL_OF_RESTAPI+"?serviceName="+DgMonitorConstants.ALERT_DEFINITION_SERVICE;
        String data = "[{\"tags\":{\"site\":\"sandbox\",\"application\":\"hdfsAuditLog\",\"policyId\":\"testHDFSPolicyCreation\",\"alertExecutorId\":\"hdfsAuditLogAlertExecutor\",\"policyType\":\"siddhiCEPEngine\"},\"description\":\"test\",\"policyDef\":\"{\\\"type\\\":\\\"siddhiCEPEngine\\\",\\\"expression\\\":\\\"from hdfsAuditLogEventStream[(timestamp == 1 or timestamp != 2 or timestamp > 3 or timestamp >= 4 or timestamp < 5 or timestamp <= 6) and (allowed == true) and (cmd == 'a' or cmd != 'b' or str:contains(cmd,'c')==true or str:regexp(cmd,'d')==true) and (host == 'a' or host != 'b' or str:contains(host,'c')==true or str:regexp(host,'d')==true) and (sensitivityType == 'a' or sensitivityType != 'b' or str:contains(sensitivityType,'c')==true or str:regexp(sensitivityType,'d')==true) and (securityZone == 'a' or securityZone != 'b' or str:contains(securityZone,'c')==true or str:regexp(securityZone,'d')==true) and (src == 'a' or src != 'b' or str:contains(src,'c')==true or str:regexp(src,'d')==true) and (dst == 'a' or dst != 'b' or str:contains(dst,'c')==true or str:regexp(dst,'d')==true) and (user == 'a' or user != 'b' or str:contains(user,'c')==true or str:regexp(user,'d')==true)] select * insert into outputStream;\\\"}\",\"notificationDef\":\"[{\\\"notificationType\\\":\\\"email\\\",\\\"sender\\\":\\\"a@example.com\\\",\\\"recipients\\\":\\\"b@example.com,c@example.com\\\",\\\"subject\\\":\\\"test\\\"},{\\\"notificationType\\\":\\\"kafka\\\",\\\"kafka_broker\\\":\\\"sandbox.hortonworks.com:6667\\\",\\\"topic\\\":\\\"test\\\"},{\\\"notificationType\\\":\\\"EagleStore\\\"},{\\\"notificationType\\\":\\\"EagleStore\\\"}]\",\"dedupeDef\":\"{\\\"alertDedupIntervalMin\\\":20}\",\"enabled\":true}]";
                EagleServiceProvider eagleServiceProvider = new EagleServiceProvider();
        try {
            HttpPost httpPost = eagleServiceProvider.createPost(restUrl, data);
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpResponse response = httpClient.execute(httpPost);
            JSONObject result = eagleServiceProvider.responseParser(response);
            httpPost.releaseConnection();
            System.out.println(result);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}