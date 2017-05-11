import java.io.RandomAccessFile;
import java.util.ArrayList;

public class RetrieveColumnInfo {

	public static ArrayList<String> getColumnNames(String schema_name, String table_name){

		ArrayList<String> columnTypes=new ArrayList<>();
		ArrayList<String> dataTypes=new ArrayList<>();
		ArrayList<Integer> dataLength=new ArrayList<>();
		ArrayList<String> Nulable=new ArrayList<>();
		ArrayList<String> Key = new ArrayList<>();
		ArrayList<String> columnNamesList=new ArrayList<>();

		try{
			RandomAccessFile columnsTableFile = new RandomAccessFile("DavisBase_schema.columns.tbl", "rw");
			int bytesRead=0;
			
			while(bytesRead<columnsTableFile.length()){
				
				String schemaNameString="";
				String tableNameString="";
				String columnNameString="";
				Integer positionInteger=0;
				String columnTypeString="";
				String NulableString="";
				String columnKeyString="";
			
				byte namelength=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength; i++){
					schemaNameString=schemaNameString+(char)columnsTableFile.readByte();
				    bytesRead++;}
				

				if(!schemaNameString.equals(schema_name))
				{
				byte namelength2=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength2; i++){
					tableNameString=tableNameString+(char)columnsTableFile.readByte();
				    bytesRead++;}
				
				byte namelength3=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength3; i++){
					columnNameString=columnNameString+(char)columnsTableFile.readByte();
				    bytesRead++;}
				
				positionInteger=columnsTableFile.readInt();
				bytesRead+=4;
				
				byte namelength4=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength4; i++){
					columnTypeString=columnTypeString+(char)columnsTableFile.readByte();
				    bytesRead++;}
				
				byte namelength5=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength5; i++){
					NulableString=NulableString+(char)columnsTableFile.readByte();
				    bytesRead++;}
				
				byte namelength6=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength6; i++){
					columnKeyString=columnKeyString+(char)columnsTableFile.readByte();
				    bytesRead++;}
				}
				

				else
				{
					byte namelength2=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength2; i++){
						tableNameString=tableNameString+(char)columnsTableFile.readByte();
					    bytesRead++;}
					

					if(!tableNameString.equals(table_name)){
					byte namelength3=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength3; i++){
						columnNameString=columnNameString+(char)columnsTableFile.readByte();
					    bytesRead++;}
					
					positionInteger=columnsTableFile.readInt();
					bytesRead+=4;
					
					byte namelength4=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength4; i++){
						columnTypeString=columnTypeString+(char)columnsTableFile.readByte();
					    bytesRead++;}
					
					byte namelength5=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength5; i++){
						NulableString=NulableString+(char)columnsTableFile.readByte();
					    bytesRead++;}
					
					byte namelength6=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength6; i++){
						columnKeyString=columnKeyString+(char)columnsTableFile.readByte();
					    bytesRead++;}
					}
					

					else
						{
						byte namelength3=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength3; i++){
							columnNameString=columnNameString+(char)columnsTableFile.readByte();
						    bytesRead++;}
						columnNamesList.add(columnNameString);
						
						
						positionInteger=columnsTableFile.readInt();
						bytesRead+=4;
		
						byte namelength4=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength4; i++){
							columnTypeString=columnTypeString+(char)columnsTableFile.readByte();
						    bytesRead++;}
						columnTypes.add(columnTypeString);
						dataTypes.add(InsertInto.parsingType(columnTypeString));
						dataLength.add(InsertInto.parsingTypeLength(columnTypeString));
						
						
						byte namelength5=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength5; i++){
							NulableString=NulableString+(char)columnsTableFile.readByte();
						    bytesRead++;}
						Nulable.add(NulableString);
						
						
						byte namelength6=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength6; i++){
							columnKeyString=columnKeyString+(char)columnsTableFile.readByte();
						    bytesRead++;}
						Key.add(columnKeyString);
						}
				}
				
			}
		}
		catch(Exception e)
		{System.out.println("Error Occurs In Retrieve Constraints: "+e.getMessage());}
		return columnNamesList;
	}
	
	public static ArrayList<String> getColumnTypes(String SchemaName, String TableName){

		//For Normal Value
		ArrayList<String> columnTypes=new ArrayList<>();
		ArrayList<String> dataTypes=new ArrayList<>();
		ArrayList<Integer> dataLength=new ArrayList<>();
		ArrayList<String> Nulable=new ArrayList<>();
		ArrayList<String> Key = new ArrayList<>();
		ArrayList<String> columnNamesList=new ArrayList<>();
		

		try{
			RandomAccessFile columnsTableFile = new RandomAccessFile("DavisBase_schema.columns.tbl", "rw");
			int bytesRead=0;
			
			while(bytesRead<columnsTableFile.length()){
				
				String schemaNameString="";
				String tableNameString="";
				String columnNameString="";
				Integer positionInteger=0;
				String columnTypeString="";
				String NulableString="";
				String columnKeyString="";
			
				byte namelength=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength; i++){
					schemaNameString=schemaNameString+(char)columnsTableFile.readByte();									
					bytesRead++;}
				

				if(!schemaNameString.equals(SchemaName))
				{
				byte namelength2=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength2; i++){
					tableNameString=tableNameString+(char)columnsTableFile.readByte();
				    bytesRead++;}
				
				byte namelength3=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength3; i++){
					columnNameString=columnNameString+(char)columnsTableFile.readByte();
				    bytesRead++;}
				
				positionInteger=columnsTableFile.readInt();
				bytesRead+=4;
				
				byte namelength4=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength4; i++){
					columnTypeString=columnTypeString+(char)columnsTableFile.readByte();
				    bytesRead++;}
				
				byte namelength5=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength5; i++){
					NulableString=NulableString+(char)columnsTableFile.readByte();
				    bytesRead++;}
				
				byte namelength6=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength6; i++){
					columnKeyString=columnKeyString+(char)columnsTableFile.readByte();
				    bytesRead++;}
				}
				

				else
				{
					byte namelength2=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength2; i++){
						tableNameString=tableNameString+(char)columnsTableFile.readByte();
					    bytesRead++;}
					

					if(!tableNameString.equals(TableName)){
					byte namelength3=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength3; i++){
						columnNameString=columnNameString+(char)columnsTableFile.readByte();
					    bytesRead++;}
					
					positionInteger=columnsTableFile.readInt();
					bytesRead+=4;
					
					byte namelength4=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength4; i++){
						columnTypeString=columnTypeString+(char)columnsTableFile.readByte();
					    bytesRead++;}
					
					byte namelength5=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength5; i++){
						NulableString=NulableString+(char)columnsTableFile.readByte();
					    bytesRead++;}
					
					byte namelength6=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength6; i++){
						columnKeyString=columnKeyString+(char)columnsTableFile.readByte();
					    bytesRead++;}
					}
					

					else
						{
						byte namelength3=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength3; i++)
						{columnNameString=columnNameString+(char)columnsTableFile.readByte();
						bytesRead++;}
						columnNamesList.add(columnNameString);
						
						
						positionInteger=columnsTableFile.readInt();
						bytesRead+=4;
		
						byte namelength4=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength4; i++)
						{columnTypeString+=(char)columnsTableFile.readByte();
						bytesRead++;}
						columnTypes.add(columnTypeString);
						dataTypes.add(InsertInto.parsingType(columnTypeString));
						dataLength.add(InsertInto.parsingTypeLength(columnTypeString));
						
						
						byte namelength5=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength5; i++)
						{NulableString=NulableString+(char)columnsTableFile.readByte();
						bytesRead++;}
						Nulable.add(NulableString);
						
						
						byte namelength6=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength6; i++)
						{columnKeyString=columnKeyString+(char)columnsTableFile.readByte();
						bytesRead++;}
						Key.add(columnKeyString);
						}
				}
				
			}
		}
		catch(Exception e)
		{System.out.println("Error Occurs In Retrieve Constraints: "+e.getMessage());}
		return dataTypes;
	}

	public static String getColumnType( String TargetColumnName, String SchemaName, String TableName){

		//For Normal Value
		ArrayList<String> columnTypes=new ArrayList<>();
		ArrayList<String> dataTypes=new ArrayList<>();
		ArrayList<Integer> dataLength=new ArrayList<>();
		ArrayList<String> Nulable=new ArrayList<>();
		ArrayList<String> Key = new ArrayList<>();
		ArrayList<String> columnNamesList=new ArrayList<>();
		

		try{
			RandomAccessFile columnsTableFile = new RandomAccessFile("DavisBase_schema.columns.tbl", "rw");
			int bytesRead=0;
			
			while(bytesRead<columnsTableFile.length()){
				
				String schemaNameString="";
				String tableNameString="";
				String columnNameString="";
				Integer positionInteger=0;
				String columnTypeString="";
				String NulableString="";
				String columnKeyString="";
			
				byte namelength=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength; i++)
				{schemaNameString=schemaNameString+(char)columnsTableFile.readByte();
				bytesRead++;}

				if(!schemaNameString.equals(SchemaName))
				{
				byte namelength2=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength2; i++)
				{tableNameString=tableNameString+(char)columnsTableFile.readByte();
				bytesRead++;}
				
				byte namelength3=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength3; i++)
				{columnNameString=columnNameString+(char)columnsTableFile.readByte();
				bytesRead++;}
				
				positionInteger=columnsTableFile.readInt();
				bytesRead+=4;
				
				byte namelength4=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength4; i++)
				{columnTypeString=columnTypeString+(char)columnsTableFile.readByte();
				bytesRead++;}
				
				byte namelength5=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength5; i++)
				{NulableString=NulableString+(char)columnsTableFile.readByte();
				bytesRead++;}
				
				byte namelength6=columnsTableFile.readByte();
				bytesRead++;
				for(int i=0 ; i<namelength6; i++)
				{columnKeyString=columnKeyString+(char)columnsTableFile.readByte();
				bytesRead++;}
				}
				

				else
				{
					byte namelength2=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength2; i++)
					{tableNameString=tableNameString+(char)columnsTableFile.readByte();
					bytesRead++;}
					

					if(!tableNameString.equals(TableName)){
					byte namelength3=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength3; i++)
					{columnNameString=columnNameString+(char)columnsTableFile.readByte();
					bytesRead++;}
					
					positionInteger=columnsTableFile.readInt();
					bytesRead+=4;
					
					byte namelength4=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength4; i++)
					{columnTypeString=columnTypeString+(char)columnsTableFile.readByte();
					bytesRead++;}
					
					byte namelength5=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength5; i++)
					{NulableString=NulableString+(char)columnsTableFile.readByte();
					bytesRead++;}
					
					byte namelength6=columnsTableFile.readByte();
					bytesRead++;
					for(int i=0 ; i<namelength6; i++)
					{columnKeyString=columnKeyString+(char)columnsTableFile.readByte();
					bytesRead++;}
					}
					
					else
						{
						byte namelength3=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength3; i++)
						{columnNameString=columnNameString+(char)columnsTableFile.readByte();
						bytesRead++;}
						if(columnNameString.equals(TargetColumnName))
						{
						positionInteger=columnsTableFile.readInt();
						bytesRead+=4;		
						byte namelength4=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength4; i++)
						{columnTypeString=columnTypeString+(char)columnsTableFile.readByte();
						bytesRead++;}
						columnTypes.add(columnTypeString);
						String currentTypeName=InsertInto.parsingType(columnTypeString);
						return currentTypeName;
						
						}
						else
						{
							positionInteger=columnsTableFile.readInt();
							bytesRead+=4;
		
							byte namelength4=columnsTableFile.readByte();
							bytesRead++;
							for(int i=0 ; i<namelength4; i++)
							{columnTypeString=columnTypeString+(char)columnsTableFile.readByte();
							bytesRead++;}
							columnTypes.add(columnTypeString);
							String currentTypeName=InsertInto.parsingType(columnTypeString);
							
							byte namelength5=columnsTableFile.readByte();
							bytesRead++;
							for(int i=0 ; i<namelength5; i++)
							{NulableString=NulableString+(char)columnsTableFile.readByte();
							bytesRead++;}
							Nulable.add(NulableString);
							
							
							byte namelength6=columnsTableFile.readByte();
							bytesRead++;
							for(int i=0 ; i<namelength6; i++)
							{columnKeyString=columnKeyString+(char)columnsTableFile.readByte();
							bytesRead++;}
							Key.add(columnKeyString);
						}
						}
				}
				
			}
		}
		catch(Exception e)
		{System.out.println("Error Occurs In Retrieve Constraints: "+e.getMessage());}
		return "";
	}




}
