import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.*;

import java.util.List;
import java.util.Map;

/**
 * Legacy approach of uploading JSON to BigQuery
 */
public class UploadFromLocalFile {

    public static void main(String... args) {

        String datasetName = "my_new_dataset";
        String tableName = "my_table";
        String projectId = "evident-ratio-333011";

        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            TableId tableId = TableId.of(projectId, datasetName, tableName);
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue("{\"name\":\"Bruce Wayne\",\"emp_id\":100100,\"company\":\"Wayne Enterprises\",\"designation\":\"CEO\"}", Map.class);

            InsertAllResponse response = bigquery.insertAll(
                    InsertAllRequest.newBuilder(tableId)
                            .addRow(map)
                            .build());
            if (response.hasErrors()) {
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    System.out.println("Response error: \n" + entry.getValue());
                }
            } else {
                System.out.println("Data");
            }
        } catch (BigQueryException e) {
            System.out.println("Insert operation failed. " + e);
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}