import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map.Entry;
import javax.print.attribute.DateTimeSyntax;

public class InsertInto {
	public static void InsertValues(String schema_name, String table_name, ArrayList<String> InsertValues){
		
		ArrayList<String> Whether_null=new ArrayList<>();
		ArrayList<String> Key = new ArrayList<>();
		ArrayList<String> columnNamesList=new ArrayList<>();
		ArrayList<String> columnTypes=new ArrayList<>();
		ArrayList<String> dataTypes=new ArrayList<>();
		ArrayList<Integer> dataLength=new ArrayList<>();

		
		try{
			RandomAccessFile columns_tableFile = new RandomAccessFile("DavisBase_schema.columns.tbl", "rw");
			int bytes_for_read=0;
			
			while(bytes_for_read<columns_tableFile.length()){
				
				String schemaNameString="";
				String tableNameString="";
				String columnNameString="";
				Integer positionInteger=0;
				String columnTypeString="";
				String NulableString="";
				String columnKeyString="";
			
				byte namelength=columns_tableFile.readByte();
				bytes_for_read++;
				for(int i=0 ; i<namelength; i++){
					schemaNameString =schemaNameString + (char)columns_tableFile.readByte();
				    bytes_for_read++;}

				if(!schemaNameString.equals(schema_name))
				{
				byte namelength2=columns_tableFile.readByte();
				bytes_for_read++;				
				for(int i=0 ; i<namelength2; i++){
					tableNameString =tableNameString + (char)columns_tableFile.readByte();
				    bytes_for_read++;}
				
				byte namelength3=columns_tableFile.readByte();
				bytes_for_read++;
				for(int i=0 ; i<namelength3; i++){
					columnNameString =columnNameString + (char)columns_tableFile.readByte();				
					bytes_for_read++;}
				
				positionInteger=columns_tableFile.readInt();
				bytes_for_read=bytes_for_read+4;
				
				byte namelength4=columns_tableFile.readByte();
				bytes_for_read++;
				for(int i=0 ; i<namelength4; i++){
					columnTypeString =columnTypeString + (char)columns_tableFile.readByte();
				    bytes_for_read++;}
				
				byte namelength5=columns_tableFile.readByte();
				bytes_for_read++;
				for(int i=0 ; i<namelength5; i++){
					NulableString=NulableString + (char)columns_tableFile.readByte();
				    bytes_for_read++;}
				
				byte namelength6=columns_tableFile.readByte();
				bytes_for_read++;
				for(int i=0 ; i<namelength6; i++){
					columnKeyString=columnKeyString + (char)columns_tableFile.readByte();
				    bytes_for_read++;}
				}
				
				else
				{
					byte namelength2=columns_tableFile.readByte();
					bytes_for_read++;
					for(int i=0 ; i<namelength2; i++){
						tableNameString =tableNameString+(char)columns_tableFile.readByte();											
						bytes_for_read++;}
					
					if(!tableNameString.equals(table_name)){
					byte namelength3=columns_tableFile.readByte();
					bytes_for_read++;
					for(int i=0 ; i<namelength3; i++){
						columnNameString=columnNameString+(char)columns_tableFile.readByte();											
						bytes_for_read++;}
					
					positionInteger=columns_tableFile.readInt();
					bytes_for_read=bytes_for_read+4;
					
					byte namelength4=columns_tableFile.readByte();
					bytes_for_read++;
					for(int i=0 ; i<namelength4; i++)
					{columnTypeString =columnTypeString+(char)columns_tableFile.readByte();
					bytes_for_read++;}
					
					byte namelength5=columns_tableFile.readByte();
					bytes_for_read++;
					for(int i=0 ; i<namelength5; i++)
					{NulableString=NulableString+(char)columns_tableFile.readByte();
					bytes_for_read++;}
					
					byte namelength6=columns_tableFile.readByte();
					bytes_for_read++;
					for(int i=0 ; i<namelength6; i++)
					{columnKeyString=columnKeyString+(char)columns_tableFile.readByte();
					bytes_for_read++;}
					}
					
					else
						{
						byte namelength3=columns_tableFile.readByte();
						bytes_for_read++;
						for(int i=0 ; i<namelength3; i++)
						{columnNameString=columnNameString+(char)columns_tableFile.readByte();
						bytes_for_read++;}
						columnNamesList.add(columnNameString);
						
						
						positionInteger=columns_tableFile.readInt();
						bytes_for_read=bytes_for_read+4;
		
						byte namelength4=columns_tableFile.readByte();
						bytes_for_read++;
						for(int i=0 ; i<namelength4; i++)
						{columnTypeString=columnTypeString+(char)columns_tableFile.readByte();
						bytes_for_read++;}
						columnTypes.add(columnTypeString);
						dataTypes.add(parsingType(columnTypeString));
						dataLength.add(parsingTypeLength(columnTypeString));
						
						byte namelength5=columns_tableFile.readByte();
						bytes_for_read++;
						for(int i=0 ; i<namelength5; i++)
						{NulableString=NulableString+(char)columns_tableFile.readByte();
						bytes_for_read++;}
						Whether_null.add(NulableString);
						
						
						byte namelength6=columns_tableFile.readByte();
						bytes_for_read++;
						for(int i=0 ; i<namelength6; i++)
						{columnKeyString=columnKeyString+(char)columns_tableFile.readByte();
						bytes_for_read++;}
						Key.add(columnKeyString);
						}
				}
				
			}
		}
		catch(Exception e)
		{System.out.println("Error Occurs In Retrieve Constraints: "+e.getMessage());}
		
		if(columnNamesList.size()==0)
		{System.out.println("column not found.");
		return;}
		
boolean whether_valid=ValuesChecking(InsertValues, columnNamesList, Whether_null, dataTypes, dataLength, Key, schema_name, table_name);

if(!whether_valid)
{	System.out.println("Your Input Is Not Valid. Insert Denied. Please Try Again.");
return;}

		try{
			RandomAccessFile tableFile=new RandomAccessFile(schema_name+"."+table_name+".tbl", "rw");
			int pointerBeforeInsertion=(int)tableFile.length();
			
			RandomAccessFile  rowInformation=new RandomAccessFile(schema_name+"."+table_name+"."+"RowInfo.ndx", "rw");
			long infoPointer=rowInformation.length();
			rowInformation.seek(infoPointer);
												
			int numValues=InsertValues.size();
			
			rowInformation.writeInt(pointerBeforeInsertion);
			rowInformation.writeInt(numValues);}
		catch(Exception e)
		{System.out.println("Error In RowInfo");}
			
		try{	
			RandomAccessFile tableFile=new RandomAccessFile(schema_name+"."+table_name+".tbl", "rw");
			int currentRow=(int)tableFile.length();
			
		for(int i=0; i<InsertValues.size(); i++)
			{
				String currentValueInString=InsertValues.get(i);
				System.out.print("value: "+currentValueInString+"\t");
				String currentColumnName=columnNamesList.get(i);
				System.out.print("column: "+currentColumnName+"\t");
				RandomAccessFile columnIndexFile = new RandomAccessFile(schema_name+"."+table_name+"."+currentColumnName+".ndx", "rw");
				String CurrentTypeName=dataTypes.get(i).toLowerCase();
				System.out.print("type name: "+CurrentTypeName+"\n");
				
				if(CurrentTypeName.equals("byte"))
				{
					Byte currentValueConverted=Byte.parseByte(currentValueInString);
					long pointer=tableFile.length();
					tableFile.seek(pointer);
					
					tableFile.writeByte(currentValueConverted);
					
					TreeMap<Byte, ArrayList<Integer>> columnIndex = new TreeMap<>();
					
					int bytesRead1=0;
					while(bytesRead1<columnIndexFile.length()){
						ArrayList<Integer> addresses=new ArrayList<>();
						Byte key=columnIndexFile.readByte();
						bytesRead1++;
						
						int times=columnIndexFile.readInt();
						bytesRead1=bytesRead1+4;
						for(int k=0; k<times; k++)
						{
							int address=columnIndexFile.readInt();
							addresses.add(address);
							bytesRead1=bytesRead1+4;
						}
						
						
						columnIndex.put(key, addresses);
					}

					boolean whether_key_in=columnIndex.containsKey(currentValueConverted);
					if(whether_key_in){

							ArrayList<Integer> begin_address=columnIndex.get(currentValueConverted);
							begin_address.add(currentRow);
							columnIndex.put(currentValueConverted, begin_address);
						
					}
					else{
						ArrayList<Integer> newAddressList=new ArrayList<>();
						newAddressList.add(currentRow);
						columnIndex.put(currentValueConverted, newAddressList);
					}
						
					RandomAccessFile columnIndexFile2=new RandomAccessFile(schema_name+"."+table_name+"."+currentColumnName+".ndx", "rw");
					for(Entry<Byte,ArrayList<Integer>> entry : columnIndex.entrySet())
					{Byte key = entry.getKey();
					columnIndexFile2.writeByte(key);
					ArrayList<Integer> AddressUpdated=entry.getValue();
					int newLength=AddressUpdated.size();
					columnIndexFile2.writeInt(newLength);
					for(int l=0;l<newLength;l++){
						columnIndexFile2.writeInt(AddressUpdated.get(l));
					}}
				}
												
				else if(CurrentTypeName.equals("int"))
				{
					Integer currentValueConverted=Integer.parseInt(currentValueInString);
					long pointer=tableFile.length();
					tableFile.seek(pointer);
					tableFile.writeInt(currentValueConverted);
					
					TreeMap<Integer, ArrayList<Integer>> columnIndex = new TreeMap<>();
					
					int bytesRead1=0;
					while(bytesRead1<columnIndexFile.length()){
						ArrayList<Integer> addresses=new ArrayList<>();
						Integer key=columnIndexFile.readInt();
						bytesRead1=bytesRead1+4;
						
						int times=columnIndexFile.readInt();
						bytesRead1=bytesRead1+4;
						for(int k=0; k<times; k++)
						{
							int address=columnIndexFile.readInt();
							addresses.add(address);
							bytesRead1=bytesRead1+4;
						}						
						
						columnIndex.put(key, addresses);
					}

					boolean keyContained=columnIndex.containsKey(currentValueConverted);
					if(keyContained){
						
							ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
							AddressOriginal.add(currentRow);
							columnIndex.put(currentValueConverted, AddressOriginal);
						
					}
					else{
						ArrayList<Integer> newAddressList=new ArrayList<>();
						newAddressList.add(currentRow);
						columnIndex.put(currentValueConverted, newAddressList);
					}
						
					RandomAccessFile columnIndexFile2=new RandomAccessFile(schema_name+"."+table_name+"."+currentColumnName+".ndx", "rw");
					for(Entry<Integer,ArrayList<Integer>> entry : columnIndex.entrySet())
					{Integer key = entry.getKey();
					columnIndexFile2.writeInt(key);
					ArrayList<Integer> AddressUpdated=entry.getValue();
					int newLength=AddressUpdated.size();
					columnIndexFile2.writeInt(newLength);
					for(int l=0;l<newLength;l++){
						columnIndexFile2.writeInt(AddressUpdated.get(l));
					}}
				}
				
				
				else if(CurrentTypeName.equals("shortint") || CurrentTypeName.equals("short"))
				{
					Short currentValueConverted=Short.parseShort(currentValueInString);
					long pointer=tableFile.length();
					tableFile.seek(pointer);
					tableFile.writeShort(currentValueConverted);
					
					TreeMap<Short, ArrayList<Integer>> columnIndex = new TreeMap<>();					
					int bytesRead1=0;
					while(bytesRead1<columnIndexFile.length()){
						ArrayList<Integer> addresses=new ArrayList<>();
						short key=columnIndexFile.readShort();
						bytesRead1 =bytesRead1+2;
						
						int times=columnIndexFile.readInt();
						bytesRead1=bytesRead1+4;
						for(int k=0; k<times; k++)
						{
							int address=columnIndexFile.readInt();
							addresses.add(address);
							bytesRead1=bytesRead1+4;
						}												
						columnIndex.put(key, addresses);
					}

					boolean keyContained=columnIndex.containsKey(currentValueConverted);
					if(keyContained){

							ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
							AddressOriginal.add(currentRow);
							columnIndex.put(currentValueConverted, AddressOriginal);
						
					}
					else{
						ArrayList<Integer> newAddressList=new ArrayList<>();
						newAddressList.add(currentRow);
						columnIndex.put(currentValueConverted, newAddressList);
					}
						
					RandomAccessFile columnIndexFile2=new RandomAccessFile(schema_name+"."+table_name+"."+currentColumnName+".ndx", "rw");
					for(Entry<Short,ArrayList<Integer>> entry : columnIndex.entrySet())
					{Short key = entry.getKey();
					columnIndexFile2.writeShort(key);
					ArrayList<Integer> AddressUpdated=entry.getValue();
					int newLength=AddressUpdated.size();
					columnIndexFile2.writeInt(newLength);
					for(int l=0;l<newLength;l++){
						columnIndexFile2.writeInt(AddressUpdated.get(l));
					}}
				}
				
				
				else if(CurrentTypeName.equals("longint") || CurrentTypeName.equals("long"))
				{
					long currentValueConverted=Long.parseLong(currentValueInString);
					long pointer=tableFile.length();
					tableFile.seek(pointer);
					tableFile.writeLong(currentValueConverted);
					
					TreeMap<Long, ArrayList<Integer>> columnIndex = new TreeMap<>();
					
					int bytesRead1=0;
					while(bytesRead1<columnIndexFile.length()){
						ArrayList<Integer> addresses=new ArrayList<>();
						long key=columnIndexFile.readLong();
						bytesRead1=bytesRead1+8;
						
						int times=columnIndexFile.readInt();
						bytesRead1=bytesRead1+4;
						for(int k=0; k<times; k++)
						{
							int address=columnIndexFile.readInt();
							addresses.add(address);
							bytesRead1=bytesRead1+4;
						}
						
						
						columnIndex.put(key, addresses);
					}

					boolean keyContained=columnIndex.containsKey(currentValueConverted);
					if(keyContained){

							ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
							AddressOriginal.add(currentRow);
							columnIndex.put(currentValueConverted, AddressOriginal);
						
					}
					else{
						ArrayList<Integer> newAddressList=new ArrayList<>();
						newAddressList.add(currentRow);
						columnIndex.put(currentValueConverted, newAddressList);
					}
						
					RandomAccessFile columnIndexFile2=new RandomAccessFile(schema_name+"."+table_name+"."+currentColumnName+".ndx", "rw");
					for(Entry<Long,ArrayList<Integer>> entry : columnIndex.entrySet())
					{Long key = entry.getKey();
					columnIndexFile2.writeLong(key);
					ArrayList<Integer> AddressUpdated=entry.getValue();
					int newLength=AddressUpdated.size();
					columnIndexFile2.writeInt(newLength);
					for(int l=0;l<newLength;l++){
						columnIndexFile2.writeInt(AddressUpdated.get(l));
					}}
				}
				
				
				else if(CurrentTypeName.equals("char")||CurrentTypeName.equals("varchar"))
				{
					String currentValueConverted=currentValueInString;
					long pointer=tableFile.length();
					tableFile.seek(pointer);

					tableFile.writeByte(currentValueConverted.length());
					tableFile.writeBytes(currentValueConverted);
					
					TreeMap<String, ArrayList<Integer>> columnIndex = new TreeMap<>();
					
					int bytesRead1=0;
					while(bytesRead1<columnIndexFile.length()){
						ArrayList<Integer> addresses=new ArrayList<>();
						String key="";
						byte length=columnIndexFile.readByte();
						bytesRead1++;
						for(int x=0; x<length;x++)
						{key+=(char)columnIndexFile.readByte();
						bytesRead1++;}
						
						int times=columnIndexFile.readInt();
						bytesRead1=bytesRead1+4;
						for(int k=0; k<times; k++)
						{
							int address=columnIndexFile.readInt();
							addresses.add(address);
							bytesRead1=bytesRead1+4;
						}
						
						
						columnIndex.put(key, addresses);
					}

					boolean keyContained=columnIndex.containsKey(currentValueConverted);
					if(keyContained){

							ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
							AddressOriginal.add(currentRow);
							columnIndex.put(currentValueConverted, AddressOriginal);
					
					}
					else{
						ArrayList<Integer> newAddressList=new ArrayList<>();
						newAddressList.add(currentRow);
						columnIndex.put(currentValueConverted, newAddressList);
					}

					RandomAccessFile columnIndexFile2=new RandomAccessFile(schema_name+"."+table_name+"."+currentColumnName+".ndx", "rw");
					for(Entry<String,ArrayList<Integer>> entry : columnIndex.entrySet())
					{String key = entry.getKey();
					columnIndexFile2.writeByte(key.length());
					columnIndexFile2.writeBytes(key);
					ArrayList<Integer> AddressUpdated=entry.getValue();
					int newLength=AddressUpdated.size();
					columnIndexFile2.writeInt(newLength);
					for(int l=0;l<newLength;l++){
						columnIndexFile2.writeInt(AddressUpdated.get(l));
					}}
				}
				
				
				else if(CurrentTypeName.equals("float"))
				{
					float currentValueConverted=Float.parseFloat(currentValueInString);
					long pointer=tableFile.length();
					tableFile.seek(pointer);
					tableFile.writeFloat(currentValueConverted);

					TreeMap<Float, ArrayList<Integer>> columnIndex = new TreeMap<>();

					int bytesRead1=0;
					while(bytesRead1<columnIndexFile.length()){
						ArrayList<Integer> addresses=new ArrayList<>();
						Float key=columnIndexFile.readFloat();
						bytesRead1=bytesRead1+4;
						
						int times=columnIndexFile.readInt();
						bytesRead1=bytesRead1+4;
						for(int k=0; k<times; k++)
						{
							int address=columnIndexFile.readInt();
							addresses.add(address);
							bytesRead1=bytesRead1+4;
						}
						
						
						columnIndex.put(key, addresses);
					}

					boolean keyContained=columnIndex.containsKey(currentValueConverted);

					if(keyContained){

							ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
							AddressOriginal.add(currentRow);
							columnIndex.put(currentValueConverted, AddressOriginal);
						
					}

					else{
						ArrayList<Integer> newAddressList=new ArrayList<>();
						newAddressList.add(currentRow);
						columnIndex.put(currentValueConverted, newAddressList);
					}
						

					RandomAccessFile columnIndexFile2=new RandomAccessFile(schema_name+"."+table_name+"."+currentColumnName+".ndx", "rw");
					for(Entry<Float,ArrayList<Integer>> entry : columnIndex.entrySet())
					{Float key = entry.getKey();
					columnIndexFile2.writeFloat(key);
					ArrayList<Integer> AddressUpdated=entry.getValue();
					int newLength=AddressUpdated.size();
					columnIndexFile2.writeInt(newLength);
					for(int l=0;l<newLength;l++){
						columnIndexFile2.writeInt(AddressUpdated.get(l));
					}}
				}
				
				
				
				else if(CurrentTypeName.equals("double"))
				{
					Double currentValueConverted=Double.parseDouble(currentValueInString);
					long pointer=tableFile.length();
					tableFile.seek(pointer);
					tableFile.writeDouble(currentValueConverted);
					
					TreeMap<Double, ArrayList<Integer>> columnIndex = new TreeMap<>();
					
					int bytesRead1=0;
					while(bytesRead1<columnIndexFile.length()){
						ArrayList<Integer> addresses=new ArrayList<>();
						Double key=columnIndexFile.readDouble();
						bytesRead1=bytesRead1+8;
						
						int times=columnIndexFile.readInt();
						bytesRead1=bytesRead1+4;
						for(int k=0; k<times; k++)
						{
							int address=columnIndexFile.readInt();
							addresses.add(address);
							bytesRead1=bytesRead1+4;
						}
						
						
						columnIndex.put(key, addresses);
					}

					boolean keyContained=columnIndex.containsKey(currentValueConverted);
					if(keyContained){

							ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
							AddressOriginal.add(currentRow);
							columnIndex.put(currentValueConverted, AddressOriginal);
						
					}
					else{
						ArrayList<Integer> newAddressList=new ArrayList<>();
						newAddressList.add(currentRow);
						columnIndex.put(currentValueConverted, newAddressList);
					}
						
					RandomAccessFile columnIndexFile2=new RandomAccessFile(schema_name+"."+table_name+"."+currentColumnName+".ndx", "rw");
					for(Entry<Double,ArrayList<Integer>> entry : columnIndex.entrySet())
					{Double key = entry.getKey();
					columnIndexFile2.writeDouble(key);
					ArrayList<Integer> AddressUpdated=entry.getValue();
					int newLength=AddressUpdated.size();
					columnIndexFile2.writeInt(newLength);
					for(int l=0;l<newLength;l++){
						columnIndexFile2.writeInt(AddressUpdated.get(l));
					}}
				}
				
				
				else if(CurrentTypeName.equals("datetime"))
				{
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
					java.util.Date date=formatter.parse(currentValueInString);
					
					long currentValueConverted=date.getTime();
					long pointer=tableFile.length();
					tableFile.seek(pointer);
					tableFile.writeLong(currentValueConverted);
					
					TreeMap<Long, ArrayList<Integer>> columnIndex = new TreeMap<>();
					
					int bytesRead1=0;
					while(bytesRead1<columnIndexFile.length()){
						ArrayList<Integer> addresses=new ArrayList<>();
						long key=columnIndexFile.readLong();
						bytesRead1=bytesRead1+8;
						
						int times=columnIndexFile.readInt();
						bytesRead1=bytesRead1+4;
						for(int k=0; k<times; k++)
						{
							int address=columnIndexFile.readInt();
							addresses.add(address);
							bytesRead1=bytesRead1+4;
						}
						
						
						columnIndex.put(key, addresses);
					}

					boolean keyContained=columnIndex.containsKey(currentValueConverted);
					if(keyContained){

							ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
							AddressOriginal.add(currentRow);
							columnIndex.put(currentValueConverted, AddressOriginal);
						
					}
					else{
						ArrayList<Integer> newAddressList=new ArrayList<>();
						newAddressList.add(currentRow);
						columnIndex.put(currentValueConverted, newAddressList);
					}
						
					RandomAccessFile columnIndexFile2=new RandomAccessFile(schema_name+"."+table_name+"."+currentColumnName+".ndx", "rw");
					for(Entry<Long,ArrayList<Integer>> entry : columnIndex.entrySet())
					{Long key = entry.getKey();
					columnIndexFile2.writeLong(key);
					ArrayList<Integer> AddressUpdated=entry.getValue();
					int newLength=AddressUpdated.size();
					columnIndexFile2.writeInt(newLength);
					for(int l=0;l<newLength;l++){
						columnIndexFile2.writeInt(AddressUpdated.get(l));
					}}
				}
				
				else if(CurrentTypeName.equals("date")) {
					SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
					java.util.Date date=formatter2.parse(currentValueInString);
					
					long currentValueConverted=date.getTime();
					long pointer=tableFile.length();
					tableFile.seek(pointer);
					tableFile.writeLong(currentValueConverted);
					
					TreeMap<Long, ArrayList<Integer>> columnIndex = new TreeMap<>();
					
					int bytesRead1=0;
					while(bytesRead1<columnIndexFile.length()){
						ArrayList<Integer> addresses=new ArrayList<>();
						long key=columnIndexFile.readLong();
						bytesRead1=bytesRead1+8;
						
						int times=columnIndexFile.readInt();
						bytesRead1=bytesRead1+4;
						for(int k=0; k<times; k++)
						{
							int address=columnIndexFile.readInt();
							addresses.add(address);
							bytesRead1=bytesRead1+4;
						}
						
						
						columnIndex.put(key, addresses);
					}

					boolean keyContained=columnIndex.containsKey(currentValueConverted);

					if(keyContained){

							ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
							AddressOriginal.add(currentRow);
							columnIndex.put(currentValueConverted, AddressOriginal);
						
					}
					else{
						ArrayList<Integer> newAddressList=new ArrayList<>();
						newAddressList.add(currentRow);
						columnIndex.put(currentValueConverted, newAddressList);
					}
						
					RandomAccessFile columnIndexFile2=new RandomAccessFile(schema_name+"."+table_name+"."+currentColumnName+".ndx", "rw");
					for(Entry<Long,ArrayList<Integer>> entry : columnIndex.entrySet())
					{Long key = entry.getKey();
					columnIndexFile2.writeLong(key);
					ArrayList<Integer> AddressUpdated=entry.getValue();
					int newLength=AddressUpdated.size();
					columnIndexFile2.writeInt(newLength);
					for(int l=0;l<newLength;l++){
						columnIndexFile2.writeInt(AddressUpdated.get(l));
					}}
				}
			}
		}
		catch(Exception e){}
		System.out.println("Insertion Success!");
		
	
		
		
	}
	


	public static boolean ValuesChecking(ArrayList<String> InsertValues, ArrayList<String> ColumnNames, ArrayList<String> Nulable,ArrayList<String> ColumnTypeName, ArrayList<Integer> ColumnTypeLength, ArrayList<String> keyList, String SchemaName, String TableName){
	
		if(InsertValues.size()>ColumnNames.size())
		{System.out.println("Error: The Number of Insert Values Exceeds Column Number.\nTable---"+TableName+" Has "+ColumnNames.size()+" Columns.\n You Inserted "+InsertValues.size()+"Values.");
		return false;}
		
		else if(InsertValues.size()<ColumnNames.size())
		{int NumInsert=InsertValues.size();
		int NumColumn=ColumnNames.size();
			for(int i=NumInsert; i<NumColumn; i++)
			{String NullStatus=Nulable.get(i).toLowerCase();
				if(NullStatus.equals("no"))
				{System.out.println("Not Null Value Missing");return false;}
			}	
		}
		
		for(int i=0; i<InsertValues.size(); i++)
		{if(keyList.get(i).toLowerCase().equals("pri"))
			{
			String currentType=ColumnTypeName.get(i).toLowerCase();
			boolean duplicate=valueDuplicate(InsertValues.get(i), currentType, SchemaName, TableName, ColumnNames.get(i));
			if(duplicate)
			{System.out.println("Key Duplicated");return false;}
			}
		}
		
		for(int i=0; i<InsertValues.size(); i++)
		{
			String currentType=ColumnTypeName.get(i).toLowerCase();
			boolean typeViolation=TypeConstraint(InsertValues.get(i), currentType);
			if(typeViolation)
			{System.out.println("Type Constraint Violation");return false;}
		}
		
		for(int i=0; i<InsertValues.size(); i++)
		{
			String currentType=ColumnTypeName.get(i).toLowerCase();
			Integer currentLengthConstraint=ColumnTypeLength.get(i);
			int currentValueLeng=InsertValues.get(i).length();
			if(currentType.equals("char"))
			{System.out.println("cahr"+InsertValues.size());
				if(currentValueLeng!=currentLengthConstraint)
				{System.out.println("Wrong Char Length. Data Length Defined="+currentLengthConstraint);
				return false;
				}
				}
			else if(currentType.equals("varchar"))
			{
				if(currentLengthConstraint<currentValueLeng)
				{System.out.println("Wrong Varchar Length. Data Length Defined="+currentLengthConstraint);
				return false;}
				}
				
		}
		
		return true;	
	}
	
	
	public static boolean valueDuplicate(String currentValue, String currentType, String SchemaName, String TableName, String ColumnName){
		
		try{
			RandomAccessFile column=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			
			if(currentType.equals("byte"))
				{int bytesRead=0; 
				
				while(bytesRead<column.length())
				{	
					byte currentByte=Byte.parseByte(currentValue);
					byte record=column.readByte();
					bytesRead++;

				if(record==currentByte)
					return true;

				int times=column.readInt();
				bytesRead=bytesRead+4;

				for(int i=0; i<times; i++)
				{column.readInt();
				bytesRead=bytesRead+4;}
					
				}
				
				}
			
			else if(currentType.equals("short")||currentType.equals("shortint"))
			{int bytesRead=0; 
			
			while(bytesRead<column.length())
			{Short currentShort=Short.parseShort(currentValue);
			Short record=column.readShort();
			bytesRead=bytesRead+2;
			if(record==currentShort)
				return true;

			int times=column.readInt();
			bytesRead=bytesRead+4;

			for(int i=0; i<times; i++)
			{column.readInt();
			bytesRead=bytesRead+4;}
				
			}
			
			}
			
			else if(currentType.equals("int"))
			{int bytesRead=0; 
			
			while(bytesRead<column.length())
			{int currentInt=Integer.parseInt(currentValue);
			int record=column.readInt();
			bytesRead=bytesRead+4;
			if(record==currentInt)
				return true;

			int times=column.readInt();
			bytesRead=bytesRead+4;
			for(int i=0; i<times; i++)
			{column.readInt();
			bytesRead=bytesRead+4;}
				
			}
			
			}
			
			else if(currentType.equals("long")||currentType.equals("longint"))
			{int bytesRead=0; 
			
			while(bytesRead<column.length())
			{long currentlong=Long.parseLong(currentValue);
			Long record=column.readLong();
			bytesRead=bytesRead+8;
			if(record==currentlong)
				return true;
			int times=column.readInt();
			bytesRead=bytesRead+4;
			for(int i=0; i<times; i++)
			{column.readInt();
			bytesRead=bytesRead+4;}
				
			}
			
			}
			
			else if(currentType.equals("double"))
			{int bytesRead=0; 
			
			while(bytesRead<column.length())
			{Double currentlong=Double.parseDouble(currentValue);
			Double record=column.readDouble();
			bytesRead=bytesRead+8;
			if(record==currentlong)
				return true;
			int times=column.readInt();
			bytesRead=bytesRead+4;
			for(int i=0; i<times; i++)
			{column.readInt();
			bytesRead=bytesRead+4;}
				
			}
			
			}
			
			else if(currentType.equals("float"))
			{int bytesRead=0; 
			
			while(bytesRead<column.length())
			{float currentfloat=Float.parseFloat(currentValue);
			Float record=column.readFloat();
			bytesRead=bytesRead+4;
			if(record==currentfloat)
				return true;
			int times=column.readInt();
			bytesRead=bytesRead+4;
			for(int i=0; i<times; i++)
			{column.readInt();
			bytesRead=bytesRead+4;}
				
			}
			
			}
			
			else if(currentType.equals("date")||currentType.equals("datetime"))
			{int bytesRead=0; 
			
			while(bytesRead<column.length())
			{Long currentDate=Long.parseLong(currentValue);
			Long record=column.readLong();
			bytesRead=bytesRead+8;
			if(record==currentDate)
				return true;
			int times=column.readInt();
			bytesRead=bytesRead+4;
			for(int i=0; i<times; i++)
			{column.readInt();
			bytesRead=bytesRead+4;}
				
			}
			
			}
			
			else if(currentType.equals("char")||currentType.equals("varchar"))
			{
					int bytesRead=0;
					while(bytesRead<column.length())
					{
						String record="";
						int recordLength=column.readByte();
						bytesRead++;
						for(int i=0; i<recordLength; i++)
						{
							record+=(char)column.readByte();
							bytesRead++;
						}
						if(record.equals(currentValue))
						{return true;}
						int times=column.readInt();
						bytesRead=bytesRead+4;
						for(int i=0; i<times; i++)
						{column.readInt();
						bytesRead=bytesRead+4;}
							
						}
					}
				
			
			
			else {
				return false;
			}
			
			
			}
		catch(Exception e)
		{e.getMessage();return false;}
		
		return false;
	}
	
	public static boolean TypeConstraint(String currentValue, String currentType){
		
		if(currentType.equals("byte")){
			try{Byte valueConverted=Byte.parseByte(currentValue);
			return false;}
			catch(Exception e)
			{System.out.println("Value: "+currentValue+"  Is Not A Byte Type!");return true;}
		}
		
		else if(currentType.equals("short")||currentType.equals("shortint")){
			try{Short valueConverted=Short.parseShort(currentValue);
			return false;}
			catch(Exception e)
			{System.out.println("Value: "+currentValue+"  Is Not A Short Type!");return true;}
		}
		
		else if(currentType.equals("int")){
			try{Integer valueConverted=Integer.parseInt(currentValue);
			return false;}
			catch(Exception e)
			{System.out.println("Value: "+currentValue+"  Is Not A Integer Type!");return true;}
		}
		
		else if(currentType.equals("long")||currentType.equals("longint")){
			try{Long valueConverted=Long.parseLong(currentValue);
			return false;}
			catch(Exception e)
			{System.out.println("Value: "+currentValue+"  Is Not A Long Type!");return true;}
		}
		
		else if(currentType.equals("float")){
			try{Float valueConverted=Float.parseFloat(currentValue);
			return false;}
			catch(Exception e)
			{System.out.println("Value: "+currentValue+"  Is Not A Float Type!");return true;}
		}
		
		else if(currentType.equals("double")){
			try{Double valueConverted=Double.parseDouble(currentValue);
			return false;}
			catch(Exception e)
			{System.out.println("Value: "+currentValue+"  Is Not A Double Type!");return true;}
		}
		
		else if(currentType.equals("datetime")){
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
			try{
				java.util.Date datetime=formatter.parse(currentValue);
				return false;
			}catch(Exception e){System.out.println("Value: "+currentValue+"  Is Not A DATETIME Type!");return true;}
		}
		
		else if(currentType.equals("date")){
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			try{
				java.util.Date datetime=formatter.parse(currentValue);
				return false;
			}catch(Exception e){System.out.println("Value: "+currentValue+"  Is Not A DATE Type!");return true;}
		}
		
		else 
			return false;
		
		
	}
	

	public static String parsingType(String rawType){
		if(rawType.contains("(")){
		String type=rawType.substring(0, rawType.indexOf('('));
		return type;}
		else return rawType;
	}
	
	public static Integer parsingTypeLength(String rawType){
		String typename=parsingType(rawType).toLowerCase();
		if(typename.equals("char") || typename.equals("varchar")){
		String temp = rawType.substring(rawType.indexOf('(')+1,rawType.indexOf(')')).replaceAll("\\s+", "");
		Integer typeLength=Integer.parseInt(temp);
		return typeLength;}
		else
		{
			if(typename.equals("byte"))
				return 1;
			else if(typename.equals("short")||typename.equals("shortint"))
				return 2;
			else if(typename.equals("int") || typename.equals("float"))
				return 4;
			else if(typename.equals("double") || typename.equals("longint") || typename.equals("long") ||typename.equals("datetime") ||typename.equals("date") )
				return 8;
			else 
				return 0;
		}
	}	

}
