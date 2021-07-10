package cbde.labs.hbase_mapreduce.reader;

public class MyHBaseReader_VerticalPartitioning extends MyHBaseReader {

	protected String[] scanFamilies() {
		//Retorna la particio que conte els 3 atributs de l'enunciat.
		return new String[]{"main_attributes"};
	}
}