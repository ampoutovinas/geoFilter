/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anmpout.geomapreducejob;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.impl.BufferedLineString;
import com.spatial4j.core.shape.impl.PointImpl;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author cloudera
 */
public class MyPath {



    private String pathId;
    private String pathName;
    private String pathOrignDeviceId;
    private String pathDestinationDeviceId;
    private BufferedLineString polyline;
   private Double distance;
 

    public MyPath() {
    }

    public static MyPath fromJSON(String jsonString) {
        if (jsonString == null) {
            return null;
        }
        MyPath returnPath = new MyPath();
        try {

            JSONObject jSONObject = new JSONObject(jsonString);
            if (jSONObject.has("Path_id") && jSONObject.optString("Path_id") != null) {
                returnPath.setPathId(jSONObject.optString("Path_id"));

            }
            if (jSONObject.has("Path_Name") && jSONObject.optString("Path_Name") != null) {
                returnPath.setPathName(jSONObject.optString("Path_Name"));

            }
            if (jSONObject.has("Path_origin_device_id") && jSONObject.optString("Path_origin_device_id") != null) {
                returnPath.setPathOrignDeviceId(jSONObject.optString("Path_origin_device_id"));

            }
            if (jSONObject.has("Path_destination_device_id") && jSONObject.optString("Path_destination_device_id") != null) {
                returnPath.setPathDestinationDeviceId(jSONObject.optString("Path_destination_device_id"));

            }
            
            if(jSONObject.has("polyline")){
            returnPath.setPolyline(createPolyline(jSONObject.optString("polyline")));
               returnPath.setDistance(calculateDistance(returnPath.getPolyline()));
            }

        } catch (JSONException ex) {
            ex.printStackTrace();
        }

        return returnPath;
    }

    public static List<MyPath> listfromJSON(String jsonString) {
        List<MyPath> returnList = new ArrayList<>();
        try {
            JSONArray JSONpaths = new JSONArray(jsonString);
            for (int i = 0; i < JSONpaths.length(); i++) {
                MyPath tmpPath = MyPath.fromJSON(JSONpaths.get(i).toString());
                if (tmpPath != null) {
                    returnList.add(tmpPath);
                }

            }
        } catch (JSONException ex) {
            ex.printStackTrace();
        }

        return returnList;

    }
    
    
        private static BufferedLineString createPolyline(String jsonString) {
                   if (jsonString == null) {
            return null;
        }
        BufferedLineString  ls = null;
         List<Point> pointsList = new ArrayList<>();
        try {
            String[] pointsArray = jsonString.split("\\s+");
            
            for (String point : pointsArray) {
              String[] Latlon =  point.split(",");
                PointImpl tmp = new PointImpl(Double.parseDouble(Latlon[0]), Double.parseDouble(Latlon[1]), SpatialContext.GEO);
              pointsList.add(tmp);
                
            }
            ls = new BufferedLineString(pointsList, 0.001, SpatialContext.GEO);
            } catch (JSONException ex) {
            ex.printStackTrace();
        }

            
            
            return ls;
        }

    public String getPathId() {
        return pathId;
    }

    public void setPathId(String pathId) {
        this.pathId = pathId;
    }

    public String getPathName() {
        return pathName;
    }

    public void setPathName(String pathName) {
        this.pathName = pathName;
    }

    public String getPathOrignDeviceId() {
        return pathOrignDeviceId;
    }

    public void setPathOrignDeviceId(String pathOrignDeviceId) {
        this.pathOrignDeviceId = pathOrignDeviceId;
    }

    public String getPathDestinationDeviceId() {
        return pathDestinationDeviceId;
    }

    public void setPathDestinationDeviceId(String pathDestinationDeviceId) {
        this.pathDestinationDeviceId = pathDestinationDeviceId;
    }

    public BufferedLineString getPolyline() {
        return polyline;
    }

    public void setPolyline(BufferedLineString polyline) {
        this.polyline = polyline;
    }

    public Double getDistance() {
        return distance;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
    }
    

    @Override
    public String toString() {
        return "MyPath{" + "pathId=" + pathId + ", pathName=" + pathName + ", pathOrignDeviceId=" + pathOrignDeviceId + ", pathDestinationDeviceId=" + pathDestinationDeviceId + ", polyline=" + polyline + '}';
    }
    
  
        private static Double calculateDistance(BufferedLineString polyline) {
           double returnDistance=0;
       
          for(int i =0;i<polyline.getPoints().size()-1;i++){
        returnDistance = returnDistance +  DistanceUtils.degrees2Dist(SpatialContext.GEO.getDistCalc().distance(polyline.getPoints().get(i), polyline.getPoints().get(i+1)), DistanceUtils.EARTH_MEAN_RADIUS_KM) * 1000;
          }       
    return returnDistance;
    }
}
