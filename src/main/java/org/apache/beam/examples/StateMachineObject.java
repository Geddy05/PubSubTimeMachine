package org.apache.beam.examples;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class StateMachineObject implements Serializable {
    private long timestamp;
    private String state;
    private int key;
    private long urgencyInMillis;

    public StateMachineObject(){};
    public StateMachineObject(long timestamp, String feature, int key, long urgencyInMillis){
        this.timestamp = timestamp;
        this.state = feature;
        this.key = key;
        this.urgencyInMillis = urgencyInMillis;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public long getUrgencyInMillis() {
        return urgencyInMillis;
    }

    public void setUrgencyInMillis(long urgencyInMillis) {
        this.urgencyInMillis = urgencyInMillis;
    }

    @JsonIgnore
    public long getTriggerInMillis(){
        return this.timestamp + this.urgencyInMillis;
    }
}
