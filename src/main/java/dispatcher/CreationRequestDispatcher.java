package contract.migration.dipatcher;

import contract.migration.data.UploadData;
import contract.migration.pipe.UploadPipeline;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;


public class CreationRequestDispatcher implements Runnable {
    private UploadPipeline input;
    private UploadPipeline output;
    private Properties props;

    public CreationRequestDispatcher(UploadPipeline input, UploadPipeline output, Properties props) {
        this.input = input;
        this.output = output;
        this.props = props;
    }

    @Override
    public void run() {
        int countDownLatch = Integer.parseInt(this.props.getProperty("countdown.number", "50"));
        long startTime = new Date().getTime();
        System.out.println("Creation Thread " + Thread.currentThread().getName() + "New Start Time: " + startTime);
        System.out.println("Launched the creation thread >>>>> " + Thread.currentThread().getName() + " Looking at  >>>>>>> " + this.input.hashCode() + " holding >>>> " + this.input.size());
        try {
            while(true) {
                UploadData data = input.pop();
                if (data == null && countDownLatch == 0) break;
                if (data == null) {
                    Thread.sleep(200);
                    countDownLatch--;
                    //System.out.println("Counting down " + Thread.currentThread().getName() + ": " + (--countDownLatch));
                    continue;
                } //count down, sleep and continue
                else { countDownLatch = Integer.parseInt(this.props.getProperty("countdown.number", "50")); }
                if (null != data && data.contract == null && data.row == -1) continue; //retries over should not be in the queue ut a defensive check anyway
                execute(data);
            }
        } catch (Exception e) {
            //ignore for the being
        }
        long endTime = new Date().getTime();
        System.out.println("Creation Thread " + Thread.currentThread().getName() + " Execution Time : " + ((endTime - startTime)/(1000 * 60)));
    }

    private void execute(UploadData data) throws JSONException {
        JSONObject json = createContract(data);
        boolean success = json.getString("status").equalsIgnoreCase("success") && json.getString("errorCode").equalsIgnoreCase("NO_ERROR") ? true : false;
        System.out.println(Thread.currentThread().getName() + " ROW # " + data.row);
        //System.out.println(Thread.currentThread().getName() + " Create ROW # " + rowIndex + "\n" + json.toString(4));
        if (success) {
            System.out.println(Thread.currentThread().getName() + " ROW # " + data.row + " Step 1: Contract Creation, Status: SUCCESS,  CONTRACT: " + json.getJSONArray("content").getJSONObject(0).getString("Name") + ",  CONTRACT URL: " + props.getProperty("base.url") + "/" + json.getJSONArray("content").getJSONObject(0).getString("Id"));
            String contractId = json.getJSONArray("content").getJSONObject(0).getString("Id");
            data.contractId = contractId;
            data.retries = Integer.valueOf(props.getProperty("num.retries", "1"));
            output.push(data);
        } else {
            //contract creation failed
            System.out.println(Thread.currentThread().getName() + " ROW # " + data.row + " Step 1: Contract Creation, Status: ERROR"
                    + ",  ERROR MESSAGE: " + json.getString("errorMessage"));
            if (--data.retries >= 0 && (json.getString("errorMessage").indexOf("UNABLE_TO_LOCK_ROW") >= 0
                    || json.getString("errorMessage").indexOf("System.LimitException") >= 0
                    || json.getString("errorMessage").indexOf("ConcurrentRequests") >= 0)) {
                input.pushRetry(data);
            }
        }
    }

    private JSONObject formatResponse(String responseString) throws JSONException{
        JSONObject json;
        if (responseString.trim().charAt(0) == '[') {
            JSONArray resArr = new JSONArray(responseString.trim());
            json = resArr.getJSONObject(0);
            json.put("status", "ERROR");
            json.put("errorMessage", json.getString("message"));
        } else {
            json = new JSONObject(responseString.trim());
        }
        return json;
    }

    private JSONObject createContract(UploadData data) throws JSONException {
        try(CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(props.getProperty("contract.url"));
            post.setHeader("Authorization", "Bearer " + data.token);
            post.setEntity(new StringEntity(data.contract.toString()));
            HttpResponse response = client.execute(post);

            HttpEntity responseEntity = response.getEntity();
            String responseString = EntityUtils.toString(responseEntity);
            return formatResponse(responseString);
        } catch (Exception e) {
            JSONObject object = new JSONObject();
            JSONArray arr = new JSONArray();
            try {
                object.put("status", "failure");
                object.put("errorCode", "Exception : " + e);
                arr.put(object);
            } catch (JSONException je) {
            }
            return formatResponse(arr.toString());
        }
    }
}
