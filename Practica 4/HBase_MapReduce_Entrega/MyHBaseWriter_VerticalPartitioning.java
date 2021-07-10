package cbde.labs.hbase_mapreduce.writer;

public class MyHBaseWriter_VerticalPartitioning extends MyHBaseWriter {

	protected String toFamily(String attribute) {
		if(attribute.equals("type") || attribute.equals("region") || attribute.equals("flav"))
			return "main_attributes";
		else return "other_attributes";
	}

	//create 'wines', 'main_attributes', 'other_attributes'
}
