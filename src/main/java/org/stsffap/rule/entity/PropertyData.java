package org.stsffap.rule.entity;


import com.alibaba.fastjson.JSONObject;

public class PropertyData {
    private String key;
    private JSONObject value;

    public static PropertyData genPropertyData() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("time", System.currentTimeMillis());
        jsonObject.put("a", 1);
        jsonObject.put("b", 2);
        jsonObject.put("c", 3);
        return new PropertyData("sys/template/device",jsonObject);
    }

    public PropertyData(String key, JSONObject value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public JSONObject getValue() {
        return value;
    }

    public void setValue(JSONObject value) {
        this.value = value;
    }
}
