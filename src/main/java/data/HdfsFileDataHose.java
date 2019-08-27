package data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HdfsFileDataHose implements DataHose {
	private final Column[] cols;
	private final String seperator;
	private final String trainPath;
	private final String evalPath;
	private final boolean skipHead;

	@JsonCreator
	public HdfsFileDataHose(
			@JsonProperty("cols") Column[] cols,
			@JsonProperty("trainPath") String trainPath,
			@JsonProperty("evalPath") String evalPath,
			@JsonProperty("seperator") String seperator,
			@JsonProperty("skipHead") boolean skipHead
	) {
		this.cols = cols;
		this.trainPath = trainPath;
		this.evalPath = evalPath;
		this.seperator = seperator;
		this.skipHead = skipHead;
	}

	@Override
	public Dataset<Row> findTrain(SparkSession session) {
		return createDF(session, trainPath);
	}

	@Override
	public Dataset<Row> findEvaluate(SparkSession session) {
		return createDF(session, evalPath);
	}

	private Dataset<Row> createDF(SparkSession session, String path) {
		return DelegateHdfsDataHose.find(session, cols, path, seperator, skipHead);
	}
}
