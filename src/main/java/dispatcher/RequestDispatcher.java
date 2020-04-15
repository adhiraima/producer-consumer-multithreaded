package contract.migration.dipatcher;

import contract.migration.data.UploadData;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class RequestDispatcher implements Runnable {
    private List<UploadData> retryContracts = Collections.synchronizedList(new LinkedList<>());
    private Properties props;
    private Map<Integer, JSONObject> contracts;
    //private static Map<String, FileWriter> logMap = new ConcurrentHashMap<>();
    private static Map<String, String> tokenMap = new ConcurrentHashMap<>();

    public RequestDispatcher(Map<Integer, JSONObject> contracts, Properties props) {
        this.contracts = contracts;
        this.props = props;
    }

    @Override
    public void run() {
        init();
        List<Integer> keyList = new ArrayList<>();
        keyList.addAll(this.contracts.keySet());
        Collections.sort(keyList);
        long timeStart = new Date().getTime();
        System.out.println("Time start main >>>>>>>>>>>>>>>>>>>>>> " + Thread.currentThread().getName() + " >>>>>> " + timeStart);
        for (Integer index : keyList) {
            try {
                execute(this.contracts.get(index), index, true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long timeEnd = new Date().getTime();
        System.out.println("Time end main >>>>>>>>>>>>>>>>>>>>>>> " + Thread.currentThread().getName() + " >>>>>> " + timeEnd);

        System.out.println("Time in minutes for main operation " + Thread.currentThread().getName() + " : " + (timeEnd - timeStart)/1000/60);

        long retryTimeStart = new Date().getTime();
        System.out.println("Time start Retry >>>>>>>>>>>>>>>>>>>>>> " + Thread.currentThread().getName() + " >>>>>> " + retryTimeStart);

        for (UploadData data : retryContracts) {
            synchronized (retryContracts) {
                if (!data.processed) {
                    try {
                        if (!data.isCreated)
                            execute(data.contract, data.row, false);
                        else if (data.isCreated && !data.streamsGenerated)
                            executeRegeneration(data.contract, data.row, data.contractId, false);
                        else if (data.isCreated && data.streamsGenerated && !data.isActivated)
                            executeActivation(data.contract, data.row, data.contractId, false);
                    } catch (Exception e) {
                        //ignore
                        e.printStackTrace();
                    }
                    data.processed = true;
                }
            }
        }
        timeEnd = new Date().getTime();
        System.out.println("Time end Retry >>>>>>>>>>>>>>>>>>>>>>> " + Thread.currentThread().getName() + " >>>>>> " + timeEnd);

        System.out.println("Time in minutes for Retry operation " + Thread.currentThread().getName() + " : " + (timeEnd - retryTimeStart)/1000/60);

        System.out.println("Total time in minutes for " + Thread.currentThread().getName() + " : " + (timeEnd - timeStart)/1000/60);
    }

    public void init() {
        try(CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(props.getProperty("auth.url"));

            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("grant_type", "password"));
            params.add(new BasicNameValuePair("client_id", props.getProperty("auth.client.Id")));
            params.add(new BasicNameValuePair("client_secret", props.getProperty("auth.client.secret")));
            params.add(new BasicNameValuePair("username", props.getProperty("auth.username")));
            params.add(new BasicNameValuePair("password", props.getProperty("auth.password")));

            post.setEntity(new UrlEncodedFormEntity(params));

            HttpResponse response = client.execute(post);

            JSONObject json = new JSONObject(EntityUtils.toString(response.getEntity()));
            tokenMap.put(Thread.currentThread().getName(), json.getString("access_token"));
        } catch (IOException | JSONException e) {
            e.printStackTrace();
            System.out.println("Unable to fetch security token for processing!");
        }
    }

    private void execute(JSONObject contract, int rowIndex, boolean firstRun) throws IOException, JSONException {
        JSONObject json = createContract(contract);
        boolean success = json.getString("status").equalsIgnoreCase("success") && json.getString("errorCode").equalsIgnoreCase("NO_ERROR") ? true : false;
        System.out.println(Thread.currentThread().getName() + " ROW # " + rowIndex);
        //System.out.println(Thread.currentThread().getName() + " Create ROW # " + rowIndex + "\n" + json.toString(4));
        if (success) {
            System.out.println(Thread.currentThread().getName() + " ROW # " + rowIndex + " Step 1: Contract Creation, Status: SUCCESS,  CONTRACT: " + json.getJSONArray("content").getJSONObject(0).getString("Name") + ",  CONTRACT URL: " + props.getProperty("base.url") + "/" + json.getJSONArray("content").getJSONObject(0).getString("Id"));
            String contractId = json.getJSONArray("content").getJSONObject(0).getString("Id");
            executeRegeneration(contract, rowIndex, contractId, firstRun);
        } else {
            //contract creation failed
            System.out.println(Thread.currentThread().getName() + " ROW # " + rowIndex + " Step 1: Contract Creation, Status: ERROR"
                    + ",  ERROR MESSAGE: " + json.getString("errorMessage"));
            if (firstRun && (json.getString("errorMessage").indexOf("UNABLE_TO_LOCK_ROW") >= 0
                    || json.getString("errorMessage").indexOf("System.LimitException") >= 0)) {
                UploadData data = new UploadData.Builder(contract, rowIndex).setActivated(false)
                                    .setCreated(false).setGenerated(false)
                                    .setRetires(Integer.valueOf(props.getProperty("retry.count", "3"))).build();;
                synchronized (retryContracts) {
                    retryContracts.add(data);
                }
            }
        }
    }

    private void executeRegeneration(JSONObject contract, int rowIndex, String contractId, boolean firstRun) throws JSONException, IOException {
        JSONObject json = generateStreams(contract, contractId);
        //logMap.get(Thread.currentThread().getName()).append("Generate Row # " + rowIndex).append("\n").write(json.toString(4));
        //System.out.println(Thread.currentThread().getName() + " Generate ROW # " + rowIndex + "\n" + json.toString(4));
        boolean success = json.getString("status").equalsIgnoreCase("success") && json.getString("errorCode").equalsIgnoreCase("NO_ERROR") ? true : false;
        if (success) {
            System.out.println(Thread.currentThread().getName() + " ROW # " + rowIndex + " Step 2: Streams Regeneration, Status: SUCCESS");
            executeActivation(contract, rowIndex, contractId, firstRun);
        } else {
            System.out.println(Thread.currentThread().getName() + " ROW # " + rowIndex + " Step 2: Streams Regeneration, Status: FAILURE  ERROR MESSAGE: " + json.getString("errorMessage"));
            //stream generation failed
            if (firstRun && (json.getString("errorMessage").indexOf("UNABLE_TO_LOCK_ROW") >= 0
                    || json.getString("errorMessage").indexOf("System.LimitException") >= 0)) {
                UploadData data = new UploadData.Builder(contract, rowIndex).setContractId(contractId)
                                    .setCreated(true)
                                    .setGenerated(false)
                                    .setActivated(false)
                                    .setRetires(Integer.valueOf(props.getProperty("retry.count", "3"))).build();
                data.isCreated = true;
                data.streamsGenerated = false;
                data.isActivated = false;
                synchronized (retryContracts) {
                    retryContracts.add(data);
                }
            }
        }
    }

    private void executeActivation(JSONObject contract, int rowIndex, String contractId, boolean firstRun) throws JSONException, IOException {
        JSONObject json = activateContract(contract, contractId);
        //logMap.get(Thread.currentThread().getName()).append("Activate Row # " + rowIndex).append("\n").write(json.toString(4));
        //System.out.println(Thread.currentThread().getName() + " Activate ROW # " + rowIndex + "\n" + json.toString(4));
        Boolean success = json.getString("status").equalsIgnoreCase("success") && json.getString("errorCode").equalsIgnoreCase("NO_ERROR") ? true : false;
        if (success)
            System.out.println(Thread.currentThread().getName() + " ROW # " + rowIndex + " Step 3: Contract Activation, Status: SUCCESS");
        else {
            System.out.println(Thread.currentThread().getName() + " ROW # " + rowIndex + " Step 3: Contract Activation, Status: FAIL" + "ERROR MESSAGE: " + json.getString("errorMessage"));
            if (firstRun && (json.getString("errorMessage").indexOf("UNABLE_TO_LOCK_ROW") >= 0
                    || json.getString("errorMessage").indexOf("System.LimitException") >= 0)) {
                UploadData data = new UploadData.Builder(contract, rowIndex).setContractId(contractId)
                                    .setCreated(true)
                                    .setGenerated(true)
                                    .setActivated(false).setRetires(Integer.valueOf(props.getProperty("retry.count", "3"))).build();
                data.isCreated = true;
                data.streamsGenerated = true;
                data.isActivated = false;
                synchronized (retryContracts) {
                    retryContracts.add(data);
                }
            }
        }
    }

    private JSONObject formatResponse(String responseString) throws JSONException{
        JSONObject json;
        if (responseString.charAt(0) == '[') {
            JSONArray resArr = new JSONArray(responseString);
            json = resArr.getJSONObject(0);
            json.put("status", "ERROR");
            json.put("errorMessage", json.getString("message"));
        } else {
            json = new JSONObject(responseString);
        }
        return json;
    }

    private JSONObject createContract(JSONObject contract) {
        try(CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(props.getProperty("contract.url"));
            post.setHeader("Authorization", "Bearer " + tokenMap.get(Thread.currentThread().getName()));
            post.setEntity(new StringEntity(contract.toString()));
            HttpResponse response = client.execute(post);

            HttpEntity responseEntity = response.getEntity();
            String responseString = EntityUtils.toString(responseEntity);
            return formatResponse(responseString);
        } catch (Exception e) {
            JSONObject object = new JSONObject();
            try {
                object.put("status", "failure");
                object.put("errorCode", "Exception : " + e);
            } catch (JSONException je) {
            }
            return object;
        }
    }

    private JSONObject generateStreams(JSONObject contract, String contractId) {
        try(CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(props.getProperty("contract.url") + "/" + contractId + "/regenerate");
            post.setHeader("Authorization", "Bearer " + tokenMap.get(Thread.currentThread().getName()));
            post.setEntity(new StringEntity(contract.toString()));
            HttpResponse response = client.execute(post);
            HttpEntity responseEntity = response.getEntity();
            String responseString = EntityUtils.toString(responseEntity);
            return formatResponse(responseString);
        } catch (Exception e) {
            JSONObject object = new JSONObject();
            try {
                object.put("status", "failure");
                object.put("errorCode", "Exception : " + e);
            } catch (JSONException je) {
            }
            return object;
        }
    }

    private JSONObject activateContract(JSONObject contract, String contractId) {
        try(CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(props.getProperty("contract.url") + "/" + contractId + "/activate");
            post.setHeader("Authorization", "Bearer " + tokenMap.get(Thread.currentThread().getName()));
            post.setEntity(new StringEntity(contract.toString()));
            HttpResponse response = client.execute(post);
            HttpEntity responseEntity = response.getEntity();
            String responseString = EntityUtils.toString(responseEntity);
            return formatResponse(responseString);
        } catch (Exception e) {
            JSONObject object = new JSONObject();
            try {
                object.put("status", "failure");
                object.put("errorCode", "Exception : " + e);
            } catch (JSONException je) {
            }
            return object;
        }
    }
}
