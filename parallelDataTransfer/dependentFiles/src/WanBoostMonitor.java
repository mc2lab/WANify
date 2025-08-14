package org.apache.spark.monitor;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.spark.compute.ComputeResourceManager;
import org.slf4j.Logger;
import org.apache.spark.network.BlockTransferService;
import org.apache.spark.network.netty.NettyBlockTransferService;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

//reading and sending block storage info
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import org.json.JSONObject;
import org.json.JSONArray;

/**
 * Created with IntelliJ IDEA.
 * User: Anonymous
 * Date: 08/15/2023 14:34 CST
 */

public class WanBoostMonitor {
    public static final Logger LOG = LoggerFactory.getLogger(WanBoostMonitor.class);
    private String optimizerPath;
    private NettyBlockTransferService blockTransferServiceClient;
    private Integer numServers;
    private String hdfsIpPath;
    //wordCount_s/wordCount600m.txt
    public WanBoostMonitor(String optimizerPath, NettyBlockTransferService blockTransferServiceClient, Integer numServers, String ipPath){
        this.optimizerPath = optimizerPath;
        this.blockTransferServiceClient = blockTransferServiceClient;
        this.numServers = numServers;
        this.hdfsIpPath = ipPath;
    }

    public void start() {
        // to poll optimizer based on a particular window or frequency interval
        System.out.println("ADM WB: Inside WANBoost Monitor. Start is invoked!");
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        Runnable task1 = new Command(this.optimizerPath, this.blockTransferServiceClient, this.numServers, this.hdfsIpPath);
        executor.scheduleAtFixedRate(task1, 0, 5, TimeUnit.SECONDS);
    }
//    public void clean() {
//        System.out.println("ADM WANify: Inside cleanup method.");
////        HashMap<String, String> inputs = new HashMap<String, String>();
////        inputs.put("name", "test");
////        Gson gson = new Gson();
////        String gsonString = gson.toJson(inputs, new TypeToken<Map<String, String>>(){}.getType());
//        ProcessBuilder processBuilder = new ProcessBuilder("sh", optimizerPath.substring(0, optimizerPath.lastIndexOf("/")+1) + "cleanup.sh");
//    }
}

class Command implements Runnable {
    static final Logger LOG = LoggerFactory.getLogger(Command.class);
    String optimizerPath;
    NettyBlockTransferService blockTransferServiceClient;
    Integer numServers;
    boolean sendBlockInfo;
    String hdfsIpPath;
    public Command(String optimizerPath, NettyBlockTransferService blockTransferServiceClient, Integer numServers, String ipPath){
        this.optimizerPath=optimizerPath;
        this.blockTransferServiceClient=blockTransferServiceClient;
        this.numServers=numServers;
        this.sendBlockInfo = true;
        this.hdfsIpPath = ipPath;
    }
    public void run() {
        // TO-DO: Set key spark.network.wanboost.optimizer.path
        LOG.warn("ADM WANify: Execution of minitor started!");
        System.out.println("ADM WB: Inside runNWBandwidthOptimizer, about to call optimizer!");
        Instant start = Instant.now();
        Gson gson = new Gson();
        HashMap<String, String> inputs = new HashMap<String, String>();
        inputs.put("name", "test");
        Map reqSizeMap = scala.collection.JavaConverters.mapAsJavaMapConverter(blockTransferServiceClient.getSize()).asJava();
        Iterator <Map.Entry<String, Long>> itr = reqSizeMap.entrySet().iterator();
        while(itr.hasNext()){
            Map.Entry<String, Long> entry = itr.next();
            LOG.warn("ADM WANify: Key is " + entry.getKey());
            LOG.warn("ADM WANify: Value is " + (Long) entry.getValue());
            if (entry.getValue() > 1000) {
                inputs.put(entry.getKey(), "" + (Long) entry.getValue());
            }
            blockTransferServiceClient.setSize(entry.getKey(), 1);
        }
        //passing block info
        if(sendBlockInfo){
            try{
                String url_link = "http://172.31.90.164:50070/webhdfs/v1/"+hdfsIpPath+"?op=GET_BlOCK_LOCATIONS";
                URL url_obj = new URL(url_link);
                HttpURLConnection conn_object = (HttpURLConnection) url_obj.openConnection();
                conn_object.setRequestMethod("GET");
                conn_object.setRequestProperty("User-Agent", "Mozilla/5.0");
                int responseVal = conn_object.getResponseCode();
                System.out.println("Response val received from WebHDFS call : " + responseVal);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(conn_object.getInputStream()));
                String ipLine;
                StringBuffer json_response = new StringBuffer();
                while ((ipLine = in.readLine()) != null) {
                    json_response.append(ipLine);
                }
                in.close();
                System.out.println("Got JSON response! printing below.");
                System.out.println(json_response.toString());
                JSONObject json_obj = new JSONObject(json_response.toString());
                System.out.println("About to get child JSON!");
                JSONArray json_obj_child = json_obj.getJSONObject("LocatedBlocks").getJSONArray("locatedBlocks");
                System.out.println("Printing parsed json array");
                System.out.println(json_obj_child.toString());
                System.out.println("Printing ipAddr of blocks");
                int totalBlocks = 0;
                for (int i = 0; i < json_obj_child.length(); i++) {
                    JSONObject json_obj_blk = json_obj_child.getJSONObject(i);
                    String ipAddOfBlock = json_obj_blk.getJSONArray("locations").getJSONObject(0).getString("ipAddr");
                    System.out.println(ipAddOfBlock);
                    if(inputs.containsKey(ipAddOfBlock+"-blk")){
                        int currBlkVal = Integer.parseInt((String) inputs.get(ipAddOfBlock+"-blk"));
                        inputs.put(ipAddOfBlock+"-blk", String.valueOf(currBlkVal+1));
                    } else {
                        inputs.put(ipAddOfBlock+"-blk", String.valueOf(1));
                    }
                    totalBlocks = totalBlocks + 1;
                    //String xyz = json_obj_blk.getString("xyz");
                }
                inputs.put("totalNumBlocks", String.valueOf(totalBlocks));
                System.out.println("ADM WB: Successfully read block info inside monitor!");
            }catch(Exception e){
                System.out.println("ADM WB: Error occurred in monitor while sending block info!");
                e.printStackTrace();
            }
            sendBlockInfo = false;
        }
        //end of passing block info
        //inputs.put("size", "1000");
        String gsonString = gson.toJson(inputs, new TypeToken<Map<String, String>>(){}.getType());

        ProcessBuilder processBuilder = new ProcessBuilder("python3", optimizerPath, gsonString);
        processBuilder.redirectErrorStream(true);

        Process process = null;
        try {
            process = processBuilder.start();
            BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));

            String strLastLine = "";
            String strCurrentLine = "";

            System.out.println("-----------------WANBoost Optimizer Input--------------------");
            System.out.println(gsonString);
            System.out.println("-----------------WANBoost Optimizer Output-------------------");

            while ((strCurrentLine = in.readLine()) != null)
            {
                System.out.println(strCurrentLine);
                strLastLine = strCurrentLine;
            }

            Map<String, Integer> decision = new Gson().fromJson(strLastLine,
                    new TypeToken<Map<String, Integer>>() {}.getType());

            for (Map.Entry<String,Integer> entry : decision.entrySet()) {
//                System.out.println("Key = " + entry.getKey() +
//                        ", Value = " + entry.getValue());
                if ((8 - blockTransferServiceClient.getMarkPortsClosed(entry.getKey())) > entry.getValue()){
                    blockTransferServiceClient.markServersClosed(entry.getKey(), (8 - blockTransferServiceClient.getMarkPortsClosed(entry.getKey())) - entry.getValue());
                } else {
                    LOG.warn("ADM WANify: entry.getKey()= "+ entry.getKey() + " entry.getValue()= "+entry.getValue()+ " numServers= "+numServers+ " getClosedVal= "+blockTransferServiceClient.getMarkPortsClosed(entry.getKey()));
                    blockTransferServiceClient.markServersOpen(entry.getKey(), entry.getValue() - (8 - blockTransferServiceClient.getMarkPortsClosed(entry.getKey())));
                }
            }
            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            System.out.println("ADM WANify: Time taken for Monitor is - "+ timeElapsed.toMillis()/1000 +" seconds");
            LOG.warn("ADM WANify: Execution of minitor ended!");

            //ADM WB: Hard coded below statement to just test out the control flow sequence!
//            if (blockTransferServiceClient.getMarkPortsClosed(ipRef) == 1){
//                blockTransferServiceClient.markServersOpen(ipRef, 1);
//            } else {
//                blockTransferServiceClient.markServersClosed(ipRef, 1);
//            }

            //return decision;
        } catch (IOException e) {
            System.out.println("ADM WANify: Some IO exception in WANify Monitor!!!");
            e.printStackTrace();
        } catch (IllegalStateException e) {
            System.out.println("ADM WANify: Some IllegalStateException in WANify Monitor!!!");
            e.printStackTrace();
        } catch(Exception e){
            System.out.println("ADM WANify: Some exception in WANify Monitor");
            e.printStackTrace();
        }

        //return null;
    }
}

