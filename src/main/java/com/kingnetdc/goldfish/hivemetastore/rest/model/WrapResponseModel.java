package com.kingnetdc.goldfish.hivemetastore.rest.model;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Created by william on 16/4/5.
 */
@XmlRootElement
public class WrapResponseModel {
    private Integer code;
    private String message;
    private Object data;
    private Object debug;

    public Object getDebug() {
        return debug;
    }

    public void setDebug(Object debug) {
        this.debug = debug;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
