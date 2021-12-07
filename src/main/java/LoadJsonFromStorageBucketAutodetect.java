import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;

// Sample to load JSON data with autodetect schema from Cloud Storage into a new BigQuery table
public class LoadJsonFromStorageBucketAutodetect {

    public static void main(String[] args) {
        String datasetName = "my_new_dataset";
        String tableName = "my_table";
        String sourceUri = "gs://cloud-samples-data/bigquery/us-states/us-states.json";
        loadJsonFromGcsAutodetect(datasetName, tableName, sourceUri);
    }

    public static void loadJsonFromGcsAutodetect(
            String datasetName, String tableName, String sourceUri) {
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            TableId tableId = TableId.of(datasetName, tableName);
            LoadJobConfiguration loadConfig =
                    LoadJobConfiguration.newBuilder(tableId, sourceUri)
                            .setFormatOptions(FormatOptions.json())
                            .setAutodetect(true)
                            .build();

            // Load data from a GCS JSON file into the table
            Job job = bigquery.create(JobInfo.of(loadConfig));
            // Blocks until this load table job completes its execution, either failing or succeeding.
            job = job.waitFor();
            if (job.isDone()) {
                System.out.println("Json Autodetect from GCS successfully loaded in a table");
            } else {
                System.out.println(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Column not added during load append \n" + e.toString());
        }
    }
}