package cbde.labs.hbase_mapreduce.writer;

import java.util.HashMap;

public class MyHBaseWriter_KeyDesign extends MyHBaseWriter {
	//Contador
	int key_q1 = 0;
	protected String nextKey() {
		String key;
		//En el cas que sigui una dada que despres haurem d'obtenir, escrivim una clau amb una z al principi
		// i utilitzem el nostre contador.
		if(this.data.get("type").equals("type_3") && this.data.get("region").equals("0")) {
			key =  "z_" + this.data.get("type") + "_" + this.data.get("region") + "_" + this.key_q1;
			this.key_q1++;
		}
		//En cas contrari utilitzem una altre clau amb el contador ja existent.
		else key = this.data.get("type") + "_" + this.data.get("region") + "_" + this.key;

		//D'aquesta manera dividim en 2 grans blocs les dades i només haurem de començar a escanejar des
		// de z_type_3_0_0 fins al final, ja que esta ordenat.
		return key;
	}
}
