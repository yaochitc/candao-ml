package data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "hdfs", value = HdfsFileDataHose.class)
})
public interface DataHose extends Serializable {
    Dataset<Row> findTrain(SparkSession session);

    Dataset<Row> findEvaluate(SparkSession session);
}
