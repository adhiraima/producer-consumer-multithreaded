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

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class ActivationRequestDispatcher implements Runnable {
    private UploadPipeline input;
    private Properties props;
    private List<Thread> parent;

    public ActivationRequestDispatcher(UploadPipeline input, Properties props, List<Thread> parent) {
        this.input = input;
        this.props = props;
        this.parent = parent;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(Integer.parseInt(this.props.getProperty("activation.warmup", "20000")));
            int countDownLatch = Integer.parseInt(this.props.getProperty("countdown.number", "50"));
            long startTime = new Date().getTime();
            System.out.println("Activation Thread " + Thread.currentThread().getName() + "New Start Time: " + startTime);
            System.out.println("Launching the Activation thread >>>> " + Thread.currentThread().getName() + " Looking at >>>> " + this.input.hashCode() + " holding >>>> " + this.input.size());
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
            long endTime = new Date().getTime();
            System.out.println("Activation Thread " + Thread.currentThread().getName() + " Execution Time : " + ((endTime - startTime)/(1000 * 60)));
        } catch (Exception e) {
            //ignore for the being
        }
    }

    private void execute(UploadData data) throws JSONException, IOException {
        JSONObject json = activateContract(data);
        //logMap.get(Thread.currentThread().getName()).append("Activate Row # " + rowIndex).append("\n").write(json.toString(4));
        //System.out.println(Thread.currentThread().getName() + " Activate ROW # " + rowIndex + "\n" + json.toString(4));
        Boolean success = json.getString("status").equalsIgnoreCase("success") && json.getString("errorCode").equalsIgnoreCase("NO_ERROR") ? true : false;
        if (success)
            System.out.println(Thread.currentThread().getName() + " ROW # " + data.row + " Step 3: Contract Activation, Status: SUCCESS");
        else {
            System.out.println(Thread.currentThread().getName() + " ROW # " + data.row + " Step 3: Contract Activation, Status: FAIL" + "ERROR MESSAGE: " + json.getString("errorMessage"));
            if (--data.retries >= 0 && (json.getString("errorMessage").indexOf("UNABLE_TO_LOCK_ROW") >= 0
                    || json.getString("errorMessage").indexOf("System.LimitException") >= 0
                    || json.getString("errorMessage").indexOf("ConcurrentRequests") >= 0)) {
                input.push(data);
            }
        }
    }

    private JSONObject activateContract(UploadData data) {
        try(CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(props.getProperty("contract.url") + "/" + data.contractId + "/activate");
            post.setHeader("Authorization", "Bearer " + data.token);
            post.setEntity(new StringEntity(data.contract.toString()));
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
}
