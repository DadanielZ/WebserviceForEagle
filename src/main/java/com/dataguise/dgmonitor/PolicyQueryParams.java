package com.dataguise.dgmonitor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by dataguise on 4/14/2016.
 */
public class PolicyQueryParams {
    private HashMap<String, String> params = new HashMap<String, String>();

    private String application = null;
    private String site = null;
    private String policyId =null;


    public String getApplicaiton(){
        return params.get("application");
    }
    public void setApplication(String application){
        params.put("application",application);
    }


    public String getSite() {
        return params.get("site");
    }

    public void setSite(String site) {
        params.put("site", site);

    }

    public String getPolicyId() {
        return params.get("policyId");
    }

    public void setPolicyId(String policyId) {
        params.put("policyId",policyId);
    }

    public String getParams(){
        String parameters = "";
        if(params.size()==0) return parameters;
        Iterator it =params.entrySet().iterator();
        if(params.size()==1) {
            Map.Entry<String,String> pair = (Map.Entry<String,String>) it.next();
            return "@"+pair.getKey()+"="+"\""+pair.getValue()+"\"";
        }
        else{
            while(it.hasNext()){
                Map.Entry<String,String> pair = (Map.Entry<String,String>) it.next();
                parameters+= "@"+pair.getKey()+"="+"\""+pair.getValue()+"\"";
                if(it.hasNext()) parameters+=" and ";
            }
            return parameters;
        }
    }
}
