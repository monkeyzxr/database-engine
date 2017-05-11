import java.io.RandomAccessFile;

public class DropTable {
	
public static boolean dropTable(String schema_name, String table_name){

		boolean whether_already_in=false;
		
		try{
			RandomAccessFile tables_table_File = new RandomAccessFile("DavisBase_schema.table.tbl", "rw");
			int bytes_for_read=0;
			
			while(bytes_for_read < tables_table_File.length()){				
				String schema_name_already="";
				byte schema_length=tables_table_File.readByte();
				bytes_for_read++;
				
				for(int i=0; i<schema_length; i++){
					schema_name_already =schema_name_already + (char)tables_table_File.readByte();
				    bytes_for_read++;}

				if(schema_name_already.equals(schema_name))
				{
				String table_name_already="";
				byte name_length=tables_table_File.readByte();
				bytes_for_read++;					
					for(int i=0; i<name_length; i++)
					{
					table_name_already =table_name_already + (char)tables_table_File.readByte();
					bytes_for_read++;
					}
					if(table_name_already.equals(table_name)){
						whether_already_in=true;
					    break;}
				}

				else
				{
				byte name_length=tables_table_File.readByte();
				bytes_for_read++;
				for(int i=0; i<name_length; i++)
					{
					tables_table_File.readByte();
					bytes_for_read++;
					}
				}
				tables_table_File.readLong();
				bytes_for_read =bytes_for_read + 8;
				}
		}
		catch(Exception e)
		{System.out.println("Error Occur In Checking Table Existance "+e.getMessage());}
		
		System.out.println(table_name+" has been dropped!");
		return whether_already_in;	
	
	
	
	
	
	
	
}

}
