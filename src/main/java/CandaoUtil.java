import data.Column;
import data.DataHose;
import data.HdfsFileDataHose;

public class CandaoUtil {
	public static DataHose getCandao() {
		String seperator = ",";
		String path = "data/candao.csv";
		Column[] avazuCols = new Column[]{
				new Column.StringColumn("date"),
				new Column.StringColumn("dayWeather"),
				new Column.StringColumn("nightWeather"),
				new Column.StringColumn("dayTemp"),
				new Column.StringColumn("nightTemp"),
				new Column.StringColumn("wind"),
				new Column.StringColumn("isWorkday"),
				new Column.StringColumn("isHoliday"),
				new Column.StringColumn("dish1"),
				new Column.StringColumn("amount1"),
				new Column.StringColumn("dish2"),
				new Column.LongColumn("amount2"),
				new Column.StringColumn("dish3"),
				new Column.LongColumn("amount3")
		};
		return new HdfsFileDataHose(avazuCols, path, path, seperator, true);
	}

	public static DataHose getCandaoByDish(String dish) {
		String seperator = ",";
		String path = String.format("data/dishes/%s.csv", dish);
		Column[] avazuCols = new Column[]{
				new Column.StringColumn("date"),
				new Column.StringColumn("dayWeather"),
				new Column.StringColumn("nightWeather"),
				new Column.StringColumn("dayTemp"),
				new Column.StringColumn("nightTemp"),
				new Column.StringColumn("wind"),
				new Column.StringColumn("isWorkday"),
				new Column.StringColumn("isHoliday"),
				new Column.DoubleColumn("amount")
		};
		return new HdfsFileDataHose(avazuCols, path, path, seperator, false);
	}

}
