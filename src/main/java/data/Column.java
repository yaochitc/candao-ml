package data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.spark.sql.types.DataType;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
		@JsonSubTypes.Type(name = "int", value = Column.IntColumn.class),
		@JsonSubTypes.Type(name = "long", value = Column.LongColumn.class),
		@JsonSubTypes.Type(name = "float", value = Column.FloatColumn.class),
		@JsonSubTypes.Type(name = "double", value = Column.DoubleColumn.class),
		@JsonSubTypes.Type(name = "string", value = Column.StringColumn.class),
		@JsonSubTypes.Type(name = "time", value = Column.TimeColumn.class),
		@JsonSubTypes.Type(name = "multi-long", value = Column.MultiLongColumn.class),
		@JsonSubTypes.Type(name = "multi-double", value = Column.MultiDoubleColumn.class),
		@JsonSubTypes.Type(name = "multi-string", value = Column.MultiStringColumn.class),
		@JsonSubTypes.Type(name = "multi-time", value = Column.MultiTimeColumn.class),

})
public abstract class Column<T> implements Serializable {
	private final String name;

	public Column(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public abstract boolean isBasicType();

	public abstract T parse(String s);

	public abstract DataType getType();

	public static class IntColumn extends Column<Integer> {
		@JsonCreator
		public IntColumn(@JsonProperty("name") String name) {
			super(name);
		}

		@Override
		public boolean isBasicType() {
			return true;
		}

		@Override
		public Integer parse(String s) {
			return Integer.parseInt(s);
		}

		@Override
		public DataType getType() {
			return ColumnTypes.integerType();
		}

	}

	public static class LongColumn extends Column<Long> {
		@JsonCreator
		public LongColumn(@JsonProperty("name") String name) {
			super(name);
		}

		@Override
		public boolean isBasicType() {
			return true;
		}

		@Override
		public Long parse(String s) {
			return Long.parseLong(s);
		}

		@Override
		public DataType getType() {
			return ColumnTypes.longType();
		}
	}

	public static class FloatColumn extends Column<Float> {
		@JsonCreator
		public FloatColumn(@JsonProperty("name") String name) {
			super(name);
		}

		@Override
		public boolean isBasicType() {
			return true;
		}

		@Override
		public Float parse(String s) {
			return Float.parseFloat(s);
		}

		@Override
		public DataType getType() {
			return ColumnTypes.floatType();
		}

	}

	public static class DoubleColumn extends Column<Double> {
		@JsonCreator
		public DoubleColumn(@JsonProperty("name") String name) {
			super(name);
		}

		@Override
		public boolean isBasicType() {
			return true;
		}

		@Override
		public Double parse(String s) {
			return Double.parseDouble(s);
		}

		@Override
		public DataType getType() {
			return ColumnTypes.doubleType();
		}
	}

	public static class StringColumn extends Column<String> {
		@JsonCreator
		public StringColumn(@JsonProperty("name") String name) {
			super(name);
		}

		@Override
		public boolean isBasicType() {
			return true;
		}

		@Override
		public String parse(String s) {
			return s;
		}

		@Override
		public DataType getType() {
			return ColumnTypes.stringType();
		}
	}


	public static class TimeColumn extends Column<Timestamp> {
		private final SimpleDateFormat sdf;

		@JsonCreator
		public TimeColumn(@JsonProperty("name") String name,
						  @JsonProperty("format") String format) {
			super(name);
			sdf = new SimpleDateFormat(format);
		}

		@Override
		public boolean isBasicType() {
			return true;
		}

		@Override
		public Timestamp parse(String s) {
			try {
				return new Timestamp(sdf.parse(s).getTime());
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public DataType getType() {
			return ColumnTypes.timestampType();
		}
	}

	public static class MultiLongColumn extends Column<Long[]> {
		private final String seperator;

		@JsonCreator
		public MultiLongColumn(@JsonProperty("name") String name,
							   @JsonProperty("seperator") String seperator) {
			super(name);
			this.seperator = seperator;
		}

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public Long[] parse(String s) {
			String[] valueStrs = s.split(seperator);
			Long[] values = new Long[valueStrs.length];
			for (int i = 0; i < valueStrs.length; i++) {
				values[i] = Long.parseLong(valueStrs[i]);
			}
			return values;
		}

		@Override
		public DataType getType() {
			return ColumnTypes.multiLongType();
		}
	}

	public static class MultiDoubleColumn extends Column<Double[]> {
		private final String seperator;

		@JsonCreator
		public MultiDoubleColumn(@JsonProperty("name") String name,
								 @JsonProperty("seperator") String seperator) {
			super(name);
			this.seperator = seperator;
		}

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public Double[] parse(String s) {
			String[] valueStrs = s.split(seperator);
			Double[] values = new Double[valueStrs.length];
			for (int i = 0; i < valueStrs.length; i++) {
				values[i] = Double.parseDouble(valueStrs[i]);
			}
			return values;
		}

		@Override
		public DataType getType() {
			return ColumnTypes.multiDoubleType();
		}
	}

	public static class MultiTimeColumn extends Column<Timestamp[]> {
		private final SimpleDateFormat sdf;
		private final String seperator;

		@JsonCreator
		public MultiTimeColumn(@JsonProperty("name") String name,
							   @JsonProperty("seperator") String seperator,
							   @JsonProperty("format") String format) {
			super(name);
			this.seperator = seperator;
			sdf = new SimpleDateFormat(format);
		}

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public Timestamp[] parse(String s) {
			try {
				String[] valueStrs = s.split(seperator);
				Timestamp[] values = new Timestamp[valueStrs.length];
				for (int i = 0; i < valueStrs.length; i++) {
					values[i] = new Timestamp(sdf.parse(valueStrs[i]).getTime());
				}
				return values;
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public DataType getType() {
			return ColumnTypes.multiTimestampType();
		}
	}

	public static class MultiStringColumn extends Column<String[]> {
		private final String seperator;

		@JsonCreator
		public MultiStringColumn(@JsonProperty("name") String name,
								 @JsonProperty("seperator") String seperator) {
			super(name);
			this.seperator = seperator;
		}

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public String[] parse(String s) {
			return s.split(seperator);
		}

		@Override
		public DataType getType() {
			return ColumnTypes.multiStringType();
		}
	}
}


