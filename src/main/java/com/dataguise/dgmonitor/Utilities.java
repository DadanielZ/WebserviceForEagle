package com.dataguise.dgmonitor;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by dataguise on 4/14/2016.
 */
public class Utilities {
    public static JSONArray strToJsonArray(String str) throws JSONException {
        //JSONObject jsonObject = new JSONObject(str);
        JSONArray jsonArray = new JSONArray(str);
        return jsonArray;
    }

    public JSONObject strToJsonObject(String str) throws JSONException {
        JSONObject jsonObject = new JSONObject(str);
        return jsonObject;

    }
}
