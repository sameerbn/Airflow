import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ReadParquetFromADLS_SP {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("Read Parquet from ADLS with SPN")
            .master("local[*]")
            .getOrCreate();

        // Set Spark conf for OAuth 2.0 with Client Credentials
        spark.conf().set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth");
        spark.conf().set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
        spark.conf().set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<client-id>");
        spark.conf().set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", "<client-secret>");
        spark.conf().set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net",
                "https://login.microsoftonline.com/<tenant-id>/oauth2/token");

        // Read Parquet from ADLS Gen2
        Dataset<Row> df = spark.read().parquet("abfss://<container>@<storage-account>.dfs.core.windows.net/path/to/file.parquet");

        df.show();
        spark.stop();
    }
}
