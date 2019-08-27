import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class TrainingUtil {
	private static SparkSession session;

	static {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[2]");
		sparkConf.setAppName("test");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.kryoserializer.buffer.max", "256m");
		session = SparkSession.builder().config(sparkConf).getOrCreate();
	}

	public static SparkSession getSparkSession() {
		return session;
	}
}
