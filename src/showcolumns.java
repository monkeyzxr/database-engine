import java.io.RandomAccessFile;

public class showcolumns {
	public static void ShowColumns(){
		try{
		RandomAccessFile columnsTableFile = new RandomAccessFile("DavisBase_schema.columns.tbl", "rw");
		
		System.out.println("TABLE_SCHEMA\tTABLE_NAME\tCOLUMN_NAME\tORDINAL_POSITION\tCOLUMN_TYPE\tIS_NULLABLE\tCOLUMN_KEY");
		System.out.println("***********************************************************************************************");
		int bytes_for_read=0;
		
		while(bytes_for_read<columnsTableFile.length()){
			String schemaNameString="";
			String tableNameString="";
			String columnNameString="";
			Integer positionInteger=0;
			String columnTypeString="";
			String Nulable="";
			String columnKey="";
			
			byte namelength=columnsTableFile.readByte();
			bytes_for_read++;
			for(int i=0 ; i<namelength; i++)
			{schemaNameString+=(char)columnsTableFile.readByte();
			bytes_for_read++;}
			System.out.print(schemaNameString+"\t");
			
			byte namelength2=columnsTableFile.readByte();
			bytes_for_read++;
			for(int i=0 ; i<namelength2; i++)
			{tableNameString+=(char)columnsTableFile.readByte();
			bytes_for_read++;}
			System.out.print(tableNameString+"\t");
			
			byte namelength3=columnsTableFile.readByte();
			bytes_for_read++;
			for(int i=0 ; i<namelength3; i++)
			{columnNameString+=(char)columnsTableFile.readByte();
			bytes_for_read++;}
			System.out.print(columnNameString+"\t");
			
			positionInteger=columnsTableFile.readInt();
			System.out.print(positionInteger+"\t");
			bytes_for_read+=4;
			
			byte namelength4=columnsTableFile.readByte();
			bytes_for_read++;
			for(int i=0 ; i<namelength4; i++)
			{columnTypeString+=(char)columnsTableFile.readByte();
			bytes_for_read++;}
			System.out.print(columnTypeString+"\t");
			
			byte namelength5=columnsTableFile.readByte();
			bytes_for_read++;
			for(int i=0 ; i<namelength5; i++)
			{Nulable+=(char)columnsTableFile.readByte();
			bytes_for_read++;}
			System.out.print(Nulable+"\t");
			
			byte namelength6=columnsTableFile.readByte();
			bytes_for_read++;
			for(int i=0 ; i<namelength6; i++)
			{columnKey+=(char)columnsTableFile.readByte();
			bytes_for_read++;}
			System.out.print(columnKey+"\t");
			System.out.println("Bytes Read: "+bytes_for_read);
		}
		System.out.println("Total size in bytes :"+columnsTableFile.length());
		}
		catch(Exception e)
		{System.out.println("Error Occurs In Showing Columns "+e.getMessage());}
	}
}
