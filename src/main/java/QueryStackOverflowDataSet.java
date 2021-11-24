import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.UUID;

public class QueryStackOverflowDataSet {
    public static void main(String... args) throws Exception {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                                "SELECT commit, author, repo_name "
                                        + "FROM `bigquery-public-data.github_repos.commits` "
                                        + "WHERE subject like '%bigquery%' "
                                        + "ORDER BY subject DESC LIMIT 10")
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.
        TableResult result = queryJob.getQueryResults();

        // Print all pages of the results.
        for (FieldValueList row : result.iterateAll()) {
            // String type
            String commit = row.get("commit").getStringValue();
            // Record type
            FieldValueList author = row.get("author").getRecordValue();
            String name = author.get("name").getStringValue();
            String email = author.get("email").getStringValue();
            // String Repeated type
            String repoName = row.get("repo_name").getRecordValue().get(0).getStringValue();
            System.out.printf(
                    "Repo name: %s Author name: %s email: %s commit: %s\n", repoName, name, email, commit);
        }
    }
}