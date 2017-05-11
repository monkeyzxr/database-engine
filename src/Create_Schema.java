import java.io.RandomAccessFile;


public class Create_Schema {
	public static void CreateSchema(String schema_name){
		
		try{			
		RandomAccessFile schema_schematable_File = new RandomAccessFile("DavisBase_schema.schemata.tbl", "rw");
		long length_now =schema_schematable_File.length();
		schema_schematable_File.seek(length_now);
		schema_schematable_File.writeByte(schema_name.length());
		schema_schematable_File.writeBytes(schema_name);
		}
		catch(Exception e){
			System.out.println("Error in "+e.getMessage());};
			
		System.out.println("New Schema '"+schema_name+"' Has Been Created!");
	}

}
