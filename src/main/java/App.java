package contract.migration;

import contract.migration.exception.ResourceMissingException;
import contract.migration.util.MigrationUtil;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws JSONException, ResourceMissingException, IOException {
        if (args.length > 0) {
            Properties properties = new Properties();
            properties.load(new FileReader(args[0] + "/app.properties"));

            System.out.println(properties.keySet());
            MigrationUtil migration = new MigrationUtil();
            Map<Integer, JSONObject> resultMaster = migration.run(properties);

            JobOrchestrator orc = new JobOrchestrator(properties);

            if (Boolean.valueOf(properties.getProperty("multi.queue", "false"))) {
                orc.init();
                System.out.println("Initialized the Multi Queue! >>>>>>>>>>>>>>>>>>>");
                orc.createPipeline(resultMaster);
                System.out.println("Populated the initial pipeline >>>>>>>>>>>>>>>>>>");
                orc.buildThreadsStack();
                System.out.println("Finished building the Thread stack >>>>>>>>>>>>>>>");
                orc.runMigrationParallelProcesses();
                System.out.println("finished the migration threads >>>>>>>>>>>>>>>>>>>");
            } else {
                orc.runMigration(resultMaster);
            }
        } else {
            System.out.println("Usage java -jar <jar-file-name>.jar <Absolute path to app.properties>");
            throw new ResourceMissingException("Count not load app.properties file for migration parameters!!");
        }
    }
}
