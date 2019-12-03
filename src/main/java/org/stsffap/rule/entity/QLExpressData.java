package org.stsffap.rule.entity;

public class QLExpressData {
    private String key;
    private String express;

    public static QLExpressData genQlExpressData(boolean flag) {
        if (flag) {
            return new QLExpressData("true", "a*b+c");
        } else {
            return new QLExpressData("false", "a+b*c");
        }
    }

    public QLExpressData(String key, String express) {
        this.key = key;
        this.express = express;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getExpress() {
        return express;
    }

    public void setExpress(String express) {
        this.express = express;
    }
}
