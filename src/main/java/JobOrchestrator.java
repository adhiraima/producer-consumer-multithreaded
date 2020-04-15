package contract.migration;

import contract.migration.data.UploadData;
import contract.migration.dipatcher.ActivationRequestDispatcher;
import contract.migration.dipatcher.CreationRequestDispatcher;
import contract.migration.dipatcher.GenerationRequestDispatcher;
import contract.migration.dipatcher.RequestDispatcher;
import contract.migration.pipe.UploadPipeline;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

public class JobOrchestrator {
    private enum ThreadType {
        CREATE, GENERATE, ACTIVATE;
    }

    private Properties props;

    private List<Thread> createThreadStack;
    private List<Thread> generationThreadStack;
    private List<Thread> activationThreadStack;

    private String accessToken;

    private final UploadPipeline creationPipeline;
    private final UploadPipeline generationPipeline;
    private final UploadPipeline activationPipeline;

    public JobOrchestrator(Properties props) {
        this.props = props;
        this.createThreadStack = new ArrayList<>();
        this.generationThreadStack = new ArrayList<>();
        this.activationThreadStack = new ArrayList<>();
        this.creationPipeline = new UploadPipeline();
        this.generationPipeline = new UploadPipeline();
        this.activationPipeline = new UploadPipeline();

    }

    public void createPipeline(Map<Integer, JSONObject> result) {
        for (Integer row : result.keySet()) {
            UploadData data = new UploadData.Builder(result.get(row), row).setActivated(false)
                    .setCreated(false).setGenerated(false).setToken(this.accessToken)
                    .setRetires(Integer.valueOf(props.getProperty("retry.count", "3"))).build();
            creationPipeline.push(data);
        }
        System.out.println("The pipeline is >>>>>>> " + creationPipeline.toString());
    }

    public void buildThreadsStack() {
        for (int i = 0; i < Integer.valueOf(props.getProperty("create.threads", "5")); i++)
            this.createThreadStack.add(new Thread(new CreationRequestDispatcher(this.creationPipeline, this.generationPipeline, this.props), "Creation_Thread_" + (i + 1)));
        for (int i = 0; i < Integer.valueOf(props.getProperty("generate.threads", "5")); i++)
            this.generationThreadStack.add(new Thread(new GenerationRequestDispatcher(this.generationPipeline, this.activationPipeline, this.props, this.createThreadStack), "Generation_Thread_" + (i + 1)));
        for (int i = 0; i < Integer.valueOf(props.getProperty("activate.threads", "5")); i++)
            this.activationThreadStack.add(new Thread(new ActivationRequestDispatcher(this.activationPipeline, this.props, this.generationThreadStack), "Activation_Thread_" + (i + 1)));
    }

    public void init() {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
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
            this.accessToken = json.getString("access_token");
        } catch (IOException | JSONException e) {
            e.printStackTrace();
            System.out.println("Unable to fetch security token for processing!");
        }
    }

    public void runMigration(Map<Integer, JSONObject> result) {
        int numThreads = Integer.valueOf(this.props.getProperty("num.threads", "5"));

        List<Map<Integer, JSONObject>> splits = new LinkedList<>();
        if (result.keySet().size() < 10) {
            numThreads = 1;
            Map<Integer, JSONObject> split = new HashMap<>();
            for (Integer row : result.keySet())
                split.put(row, result.get(row));
            splits.add(split);
        } else {
            for (Integer row : result.keySet()) {
                if (row <= numThreads) {
                    Map<Integer, JSONObject> mp = new HashMap<>();
                    mp.put(row, result.get(row));
                    splits.add(mp);
                } else {
                    int index = row;
                    while (index > numThreads)
                        index %= numThreads;
                    splits.get(index).put(row, result.get(row));
                }
            }
        }
        List<Thread> workers = new LinkedList<>();
        for (int i = 0; i < numThreads; i++) {
            RequestDispatcher dispatcher = new RequestDispatcher(splits.get(i), this.props);
            Thread thread = new Thread(dispatcher, "Migration_Thread_" + (i + 1));
            workers.add(thread);
        }
        workers.forEach(Thread::start);
    }

    public void runMigrationParallelProcesses() {
        this.createThreadStack.forEach(Thread::start);
        this.generationThreadStack.forEach(Thread::start);
        this.activationThreadStack.forEach(Thread::start);
    }
}
