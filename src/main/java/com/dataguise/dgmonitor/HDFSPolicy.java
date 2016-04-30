package com.dataguise.dgmonitor;

import java.util.*;

/**
 * Parameters to create HDFSPolicy
 *
 */
public class HDFSPolicy {
    /**In the parameterHashMap each entry stores a Match Criteria */
    private HashMap<String,Set<String>> parameterHashMap = new HashMap<String, Set<String>>();

    /**indicates whether a hdfs commands succeed or fail, "true" means success, "false" means failed**/
    private Boolean allowed = null;

    /**site name **/
    private String site = "";

    /**policy name**/
    private String policyId = "";

    /**name of application, for HDFS policy by default it is hdfsAuditLog**/
    private String applicaiton = "hdfsAuditLog";

    /**alertExecutorId**/
    private String alertExecutorId = "hdfsAuditLogAlertExecutor";

    /**policyType**/
    private String policyType = "siddhiCEPEngine";

    /**description of this policy*/
    private String description = "";

    /**senders email address*/
    private String sender = "";

    /**recipients email address*/
    private String recipients = "";

    /**subject of email*/
    private String subject = "";

    /**by default it is email, and up to Eagle 0.3, only support email**/
    private String flavor = "email";

    /**email id**/
    private String id = "email_1";

    /**enable this policy or not: use "true" to enable and "false" to disable*/
    private String enabled = "true";

    public void setSite(String site){
        this.site = site;
    }

    public void setAllowed(Boolean allowed){
        this.allowed = allowed;
    }

    public void setCmd(Set<String> cmd){
        parameterHashMap.put("cmd",cmd);
    }

    public void setDst(Set<String> dst){
        parameterHashMap.put("dst",dst);
    }

    public void setHost(Set<String> host){
        parameterHashMap.put("host",host);
    }

    public void setSecurityZone(Set<String> securityZone){
        parameterHashMap.put("securityZone",securityZone);
    }

    public void setSensitivityType(Set<String> sensitivityType){
        parameterHashMap.put("sensitivityType",sensitivityType);
    }

    public void setSrc(Set<String> src){
        parameterHashMap.put("src",src);
    }

    public void setTimeStamp(Set<String> timeStamp){
        parameterHashMap.put("timeStamp",timeStamp);
    }

    public void setUser(Set<String> user){
        parameterHashMap.put("user",user);
    }



    public void setDescription(String description){
        this.description = description;
    }

    public void setSender(String sender){
        this.sender = sender;
    }

    public void setRecipients(String recipients){
        this.recipients = recipients;
    }

    public void setSubject(String subject){
        this.subject = subject;
    }

    public void setFlavor(String flavor){
        this.flavor = flavor;
    }

    public void setId(String id){
        this.id = id;
    }

    public void setEnabled(boolean enabled){
        if(enabled) this.enabled="true";
        else this.enabled = "false";
    }

    //get parameter value

    public String getAllowed(){
        String result = null;
        if(this.allowed==null) return result;
        if (allowed) return new String("true");
        else return new String("false");
    }

    public Set<String> getCmd() {
        return parameterHashMap.get("cmd");
    }

    public Set<String> getDst(){
        return parameterHashMap.get("dst");
    }

    public Set<String> getHost(){
        return parameterHashMap.get("host");
    }

    public Set<String> getSecurityZone(){
        return parameterHashMap.get("SecurityZone");
    }

    public Set<String> getSensitivityType(){
        return parameterHashMap.get("sensitivityType");
    }

    public Set<String> getSrc(){
        return parameterHashMap.get("src");
    }

    public Set<String> getTimeStamp(){
        return parameterHashMap.get("timeStamp");
    }

    public Set<String> getUser(){
        return parameterHashMap.get("user");
    }

    public String getDescription(){
        return description;
    }

    public String getSender(){
        return sender;
    }

    public String getRecipients(){
        return recipients;
    }

    public String getSubject(){
        return subject;
    }

    public String getFlavor(){
        return flavor;
    }

    public String getId(){
        return id;
    }

    public String getSite(){
        return site;
    }

    public String getEnabled(){
        return enabled;
    }

    /**
     * Construct query expression based on this HDFS policy parameters
     * */
    public String getQueryExpression() throws Exception{
        String expression = "";
        if(getSite()==null||getPolicyId()==null) throw new Exception("PolicyID or Site not set!");
        if(parameterHashMap.size()==0) {
            if(getAllowed()==null) throw new Exception("No parameter set for this policy");
            else return "("+"allowed == "+getAllowed()+")";
        }

        String tmp = "";
        if(getAllowed()!=null){
            tmp =  "("+"allowed == "+getAllowed()+") and";
        }

        Set set = parameterHashMap.entrySet();
        Iterator iterator = set.iterator();
        Map.Entry entry = (Map.Entry) iterator.next();
        expression = addParentheses(getExpression(entry));

        if(parameterHashMap.size()==1) {
            return tmp+expression;
        }
        else{
            while(iterator.hasNext()){
                expression+= " and ";
                entry = (Map.Entry) iterator.next();
                String tmpExp = getExpression(entry);
                expression+=addParentheses(tmpExp);
            }
        }
        return expression;
    }

    private String getExpression(Map.Entry entry){
        String key = (String) entry.getKey();
        Set<String> values = (Set<String>) entry.getValue();
        String expression = "";

        Iterator iterator = values.iterator();
        //construct the first element in eagle query
        if(iterator.hasNext()){
            String str = (String) iterator.next();
            if(key.equals("timeStamp"))expression =  key+" == "+str;
            else expression =  key+" == "+"'"+str +"'";
        }
        if(values.size()==1) return expression;

        while(iterator.hasNext()){
            String str = (String) iterator.next();
            if(key.equals("timeStamp")){
                expression = expression + " or " + key + " == "+ str;
            }
            else {
                expression = expression + " or "+ key + " == "+"'"+str+"'" ;
            }
        }
        return expression;
    }

    private String addParentheses(String str){
        return "("+str+")";
    }

    public String getApplicaiton() {
        return applicaiton;
    }

    public void setApplicaiton(String applicaiton) {
        this.applicaiton = applicaiton;
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public String getAlertExecutorId() {
        return alertExecutorId;
    }

    public void setAlertExecutorId(String alertExecutorId) {
        this.alertExecutorId = alertExecutorId;
    }

    public String getPolicyType() {
        return policyType;
    }

    public void setPolicyType(String policyType) {
        this.policyType = policyType;
    }

    public String getJSONFormat() throws Exception {
        String queryExpression;
        String notificationDef = "\\\"sender\\\":\\\"" + getSender() +"\\\"," +
                "\\\"recipients\\\":\\\"" + getRecipients() +"\\\"," +
                "\\\"subject\\\":\\\"" + getSubject() +"\\\"," +
                "\\\"flavor\\\":\\\"" + getFlavor() +"\\\"," +
                "\\\"id\\\":\\\"" + getId() +"\\\"";
        queryExpression = getQueryExpression();
        String jsonData = "{\"tags\":{\"site\":\""+getSite()+"\",\"application\":\""+getApplicaiton()+"\",\"policyId\": \""+getPolicyId()+"\",\"alertExecutorId\":\""+getAlertExecutorId()+"\", \"policyType\": \""+getPolicyId()+"\"},\"desc\":\""+getDescription()+"\", \"policyDef\": \"{\\\"type\\\": \\\""+getPolicyType()+"\\\", \\\"expression\\\": \\\"from hdfsAuditLogEventStream["+queryExpression+"] select * insert into outputStream;\\\"}\",\"notificationDef\":\"[{"+notificationDef+"}]\",\"enabled\":"+getEnabled()+"}";
        return jsonData;
    }
}
