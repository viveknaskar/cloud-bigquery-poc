import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;

public class CreateDataSet {
    public static void main(String... args) throws Exception {
        /**
         * Instantiate a client. If you don't specify credentials when constructing a client, the
         * client library will look for credentials in the environment
         */
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        String datasetName = "my_new_dataset";

        Dataset dataset = null;
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();

        dataset = bigquery.create(datasetInfo);

        System.out.printf("Dataset %s created.%n", dataset.getDatasetId().getDataset());
    }
}