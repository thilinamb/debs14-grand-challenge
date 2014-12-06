package edu.colostate.cs.storm.util;

import java.util.HashSet;
import java.util.Set;

/**
 * Author: Thilina
 * Date: 12/6/14
 */
public class OutlierTracker {
    private Set<String> completeSet = new HashSet<String>();
    private Set<String> outlierSet = new HashSet<String>();

    public void addMember(String key){
        completeSet.add(key);
    }

    public void addOutlier(String key){
        outlierSet.add(key);
    }

    public void removeOutlier(String key){
        outlierSet.remove(key);
    }

    public boolean isOutlier(String key){
        return outlierSet.contains(key);
    }

    public boolean isMember(String key){
        return completeSet.contains(key);
    }

    public double getCurrentPercentage(){
        return (outlierSet.size() * 1.0)/(completeSet.size());
    }
}
