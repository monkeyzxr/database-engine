import java.io.RandomAccessFile;

public class UseSchema {

	public static boolean useSchema(String schemanamestring){
		
		boolean whether_in=false;
		try{
		
		RandomAccessFile schemataTableFile = new RandomAccessFile("DavisBase_schema.schemata.tbl", "rw");
		byte bytes_for_read=0;
		
		while(bytes_for_read<schemataTableFile.length()){
			String temp_schemaName="";
			byte varcharlength=schemataTableFile.readByte();
			bytes_for_read++;
			for(int i=0; i<varcharlength; i++){
				temp_schemaName+=(char)schemataTableFile.readByte();
				bytes_for_read++;
			}
			if(temp_schemaName.equals(schemanamestring)){
				whether_in=true;
				break;
			}
			
		}
		
		}
		catch(Exception e)
		{System.out.println("Error Occurs In Searching Schema: "+e.getMessage());}
		return whether_in;
	}
	
}
