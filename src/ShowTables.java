import java.io.RandomAccessFile;

public class ShowTables {
	public static void showTables(String schema_name){
		if(schema_name.equals(""))
		{System.out.println("Please Select A Schema First...");}
		
		else{
			System.out.println("TABLE_NAMES\n**********");
		try{
			int bytes_for_read=0;

			RandomAccessFile tablesTableFile = new RandomAccessFile("DavisBase_schema.table.tbl", "rw");
			
			while(bytes_for_read<tablesTableFile.length()){
				
				String chosen_schemaname="";
				byte schema_length=tablesTableFile.readByte();
				bytes_for_read++;
				for(int i=0; i<schema_length; i++){
					chosen_schemaname=chosen_schemaname+(char)tablesTableFile.readByte();
				    bytes_for_read++;}

				if(chosen_schemaname.equals(schema_name))
				{

					String chosen_tablename="";

					byte name_length=tablesTableFile.readByte();
					bytes_for_read++;

					for(int i=0; i<name_length; i++)
					{
						chosen_tablename=chosen_tablename+(char)tablesTableFile.readByte();
						bytes_for_read++;
					}

					System.out.println(chosen_tablename);
				}
				else
				{
					byte nameLength=tablesTableFile.readByte();
					bytes_for_read++;
					for(int i=0; i<nameLength; i++)
					{
						tablesTableFile.readByte();
						bytes_for_read++;
					}
				}

				tablesTableFile.readLong();
				bytes_for_read=bytes_for_read+8;
				
			}
				}
		catch(Exception e)
		{System.out.println("Error Occurs In Showing Tables: "+e.getMessage());}
		}
	}
}
