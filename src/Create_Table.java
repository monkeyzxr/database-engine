import java.io.RandomAccessFile;

import java.util.ArrayList;

public class Create_Table {
	
	public static boolean checkTableExistance(String table_name, String schema_name){
		boolean whether_already_in=false;
		
		try{
			RandomAccessFile tablesTable_File = new RandomAccessFile("DavisBase_schema.table.tbl", "rw");
			int bytes_for_read=0;
			
				while(bytes_for_read < tablesTable_File.length()){
				
				String schema_name_alreadyin ="";
				byte schema_length = tablesTable_File.readByte();
				bytes_for_read++;
				for(int i=0; i< schema_length; i++){
					schema_name_alreadyin = schema_name_alreadyin + (char)tablesTable_File.readByte();
				    bytes_for_read++;}

				if(schema_name_alreadyin.equals(schema_name))
				{
					String table_name_alreadyin="";
					byte name_length=tablesTable_File.readByte();
					bytes_for_read++;
					
					for(int i=0; i<name_length; i++)
					{
					table_name_alreadyin =table_name_alreadyin + (char)tablesTable_File.readByte();
					bytes_for_read++;
					}

					if(table_name_alreadyin.equals(table_name)){
						whether_already_in = true;
					    break;}
				}

				else
				{
					byte name_length=tablesTable_File.readByte();
					bytes_for_read++;
					for(int i=0; i < name_length; i++)
					{
					tablesTable_File.readByte();
					bytes_for_read++;
					}
				}

				tablesTable_File.readLong();
				bytes_for_read =bytes_for_read + 8;
				}
		}
		catch(Exception e)
		{System.out.println("Error Occur In Checking Table Existance "+e.getMessage());}
		
		
		return whether_already_in;	
	}
	
	
	
	public static void CreateTable(String schema_name, String table_name, ArrayList<String> column_names, ArrayList<String> column_types, ArrayList<String> column_null_status, ArrayList<String> column_keys){
		Integer columnnumber=column_names.size();
		
		if(checkTableExistance(table_name, schema_name))
		System.out.println("Table '"+table_name+"' Has Already Been In Schema '"+schema_name+"'.");
		
		else{
		try{
			RandomAccessFile new_table_File = new RandomAccessFile(schema_name+"."+table_name+".tbl", "rw");
		}
		catch(Exception e)
		{System.out.println("Error Occurs IN Creating New Table: "+e.getMessage());}
		
		try{
			RandomAccessFile tables_table_File = new RandomAccessFile("DavisBase_schema.table.tbl", "rw");	
			long length_now=tables_table_File.length();
			tables_table_File.seek(length_now);
			tables_table_File.writeByte(schema_name.length());
			tables_table_File.writeBytes(schema_name);
			tables_table_File.writeByte(table_name.length());
			tables_table_File.writeBytes(table_name);
			tables_table_File.writeLong(0);
		}
		catch(Exception e)
		{System.out.println("Error Occurs In Iserting table information Into DavisBase_schema"+e.getLocalizedMessage());}
		

		try{
			RandomAccessFile columns_table_File = new RandomAccessFile("DavisBase_schema.columns.tbl", "rw");
			Long now_Length=columns_table_File.length();
			columns_table_File.seek(now_Length);
		for(int i=0; i<columnnumber; i++){
			String column_name_now=column_names.get(i);
			String column_type_now=column_types.get(i);
			String Key="";
			String whether_can_null = "YES";

			if(column_null_status.get(i).toLowerCase().equals("not null"))
				whether_can_null = "NO";
			if(column_keys.get(i).toLowerCase().equals("primary key"))
			{Key="PRI";
			whether_can_null = "NO";}
			
			int temp=i+1;
			columns_table_File.writeByte(schema_name.length());
			columns_table_File.writeBytes(schema_name);
			columns_table_File.writeByte(table_name.length());
			columns_table_File.writeBytes(table_name);
			columns_table_File.writeByte(column_name_now.length());
			columns_table_File.writeBytes(column_name_now);
			columns_table_File.writeInt(temp);
			columns_table_File.writeByte(column_type_now.length());
			columns_table_File.writeBytes(column_type_now);
			columns_table_File.writeByte(whether_can_null.length());
			columns_table_File.writeBytes(whether_can_null);
			columns_table_File.writeByte(Key.length());
			columns_table_File.writeBytes(Key);
			
			RandomAccessFile newfile = new RandomAccessFile(schema_name+"."+table_name+"."+column_name_now+".ndx", "rw");
			
		}
		}
		catch(Exception e)
		{System.out.println("Error Occurs In Inserting Into schema_DavisBase_column table: "+e.getLocalizedMessage());}
	}
	}
		
}
