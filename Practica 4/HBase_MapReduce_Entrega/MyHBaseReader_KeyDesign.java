package cbde.labs.hbase_mapreduce.reader;

public class MyHBaseReader_KeyDesign extends MyHBaseReader {

	protected String scanStart() {
		//El primer valor que haurem de buscar.
		return "z_type_3_0_0";
	}

	protected String scanStop() {
		//No cal retornar cap valor en concret perque ha d'agafar totes les keys des de scanStart fins al final
		return null;
	}

}
