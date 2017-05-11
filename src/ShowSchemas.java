import java.io.RandomAccessFile;

public class ShowSchemas {
	
	public static void showSchemas(){
		System.out.println("SCHEMAS\n#####################");
		try{
			RandomAccessFile schemataTableFile = new RandomAccessFile("DavisBase_schema.schemata.tbl", "rw");	
			int bytes_for_read=0;
			while(bytes_for_read<schemataTableFile.length()){
				byte varcharlength = schemataTableFile.readByte();
				bytes_for_read++;
				for(int i=0; i<varcharlength; i++){
					System.out.print((char)schemataTableFile.readByte());
					bytes_for_read++;
				}
				System.out.println();
			}
		}
		catch(Exception e)
		{System.out.println("Error Occurs In Show Schemas: "+e.getMessage());}
	}
}
