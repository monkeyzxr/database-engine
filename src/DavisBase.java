import java.util.ArrayList;
import java.util.Scanner;

public class DavisBase {
	public static void main(String [] args){
		
		System.out.println("Welcom To Davis Base.");
		System.out.println("Type \"help;\" to display all supported commands.");
		System.out.println("****************************************************************************************");
		
		final String helpinfo="Type 'show schemas;' to display all schama in database.\n"
				+ "Type 'show tables;' to display all tables of a schema in database\n"
				+ "Type 'show columns;' to display all the columns information in database\n"
				+ "Type 'use + schemaname;' to use schema which you choose\n"
				+ "Type 'create schema + schemaname' to create a new schema in given name\n"
				+ "Type 'create table + tablename +(columnname datatype [primary key|not null],```columnnameN datatype(datalength) [|])' to create a table\n"
				+ "(Notice: key and null constraint must be specified in [primary key (or blank)|not null (or blank)]!)\n"
				+ "Type insert into +tablename values(value1,value2,```valueN) to insert values in specified table\n"
				+ "Type 'drop table + tablename;' to drop table\n"
				+ "Type 'select * from +tablename+ where + columnname =/<>/</> + comapre value' to show rows in specified table\n"
				+ "Type 'exit;' to leave DavisBase\n"
				+ "*****************************************************************************";
		
		String schema_now="";
		
		
		
		Scanner scanner = new Scanner(System.in).useDelimiter(";");
		String userCommand;
		
		do{userCommand = scanner.next().trim();
		
		if(!userCommand.toLowerCase().equals("exit")){
		int command_number = CommandParser.myParser(userCommand);
		
		switch (command_number) {		
		case 1:  //show schemas
			ShowSchemas.showSchemas();
			break;

		case 2:	 //show tables
			ShowTables.showTables(schema_now);
			break;

		case 3:   //use a schema
			String schema_name_now=CommandParser.parseUse(userCommand);
			boolean whether_in=UseSchema.useSchema(schema_name_now);
			if(whether_in)
			{schema_now=schema_name_now;
			System.out.println("You are now using schema  |"+schema_now+"|");}
			else 
			{
				System.out.println("Error, cannot find schema names: "+schema_name_now);
			}
			break;

		case 4:  	//build a new schema
			String new_schama_name=CommandParser.parseCreateSchema(userCommand);
			Create_Schema.CreateSchema(new_schama_name);
			break;
	
		case 5:	 //build a new table
			if(schema_now.equals("")){
				System.out.println("Please Choose A Schema First");
			    break;}
			
			String table_name=CommandParser.parseCreateTableTName(userCommand);
			ArrayList<String> column_names=CommandParser.parseCreateTableColumnNames(userCommand);
			ArrayList<String> column_types=CommandParser.parseCreateTableDataTypes(userCommand);
			boolean whether_correct=TableCreationValidation(column_types);
			if(whether_correct){				
			    ArrayList<String> column_if_null=CommandParser.parseCreateTableNull(userCommand);
			    ArrayList<String> key_in_column=CommandParser.parseCreateTableKey(userCommand);
			    Create_Table.CreateTable(schema_now,table_name,column_names,column_types,column_if_null,key_in_column);
			    System.out.println("Table  |"+table_name+"|  has been created.");
			    break;}
			else {
				System.out.println("Please try again");
				break;
			}
		
		case 6:  //insert into values
			String insert_into_table=CommandParser.parseInsertTableName(userCommand);
			ArrayList<String> values_for_insert=CommandParser.parseInsertValues(userCommand);
			if(schema_now.equals("")){
				System.out.println("Please Choose A Schema First");
			    break;}
			//check whether table exist
			boolean whether_table_exist=Create_Table.checkTableExistance(insert_into_table, schema_now);
			if(!whether_table_exist){
				System.out.println("Table "+insert_into_table+"Doesn't Exist.");
			    break;}
			InsertInto.InsertValues(schema_now, insert_into_table, values_for_insert);			
			break;

		case 7:   //select a record from a table
			if(schema_now.equals("")){
				System.out.println("Please Choose A Schema First");
			    break;}
			String select_table=CommandParser.parseSelectTableName(userCommand);
			boolean whether_table_in=Create_Table.checkTableExistance(select_table, schema_now);
			if(!whether_table_in){
				System.out.println("Table "+select_table+"Doesn't Exist.");
			    break;}
			String comp_value=CommandParser.parseSelectValue(userCommand);
			Integer comp_symbol=CommandParser.parseSelectComparison(userCommand);
			String ColumnName=CommandParser.parseSelectColumnName(userCommand);		
			ArrayList<Integer> locations=SelectFrom.getLocations(schema_now, select_table, ColumnName, comp_value, comp_symbol);
			SelectFrom.printRow(schema_now, select_table, locations);			
			break;
		
		case 8: //show columns information
			showcolumns.ShowColumns();
			break;
			
		case 9: // display help
			System.out.println(helpinfo);
			break;
			
		case 10: // drop tables
			if(schema_now.equals("")){
				System.out.println("Please Choose A Schema First");
			    break;}
			String table_for_drop=CommandParser.parseDropTableName(userCommand);
			boolean whether_table_alreadyexist=Create_Table.checkTableExistance(table_for_drop, schema_now);
			if(!whether_table_alreadyexist){
				System.out.println("Table "+table_for_drop+"Doesn't Exist.");
			    break;}			
			DropTable.dropTable(schema_now, table_for_drop);			
		break;
		
		case 0:
			System.out.println("Sorry, I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
		}
		}
		while(!userCommand.equals("exit"));
		System.out.println("Now Exiting...");
		
		try{
			
		}
		catch(Exception e)
		{System.out.println("Error Occurs In DavisBase_schema. "+e.getMessage());}
	}
	
	
	////////////////////////////////////////////////////////////////////////////////////
	//check table's validation
	public static boolean TableCreationValidation(ArrayList<String> column_types)
	{
		boolean whether_right=true;
		
		for(int i = 0; i < column_types.size(); i++)
		{
			
			String type_name_now=column_types.get(i).toLowerCase();
			
			if(!type_name_now.contains("byte") && !type_name_now.contains("int") 
					&& !type_name_now.contains("short") && !type_name_now.contains("short int") 
					&& !type_name_now.contains("long") && !type_name_now.contains("long int") 
					&& !type_name_now.contains("char(") && !type_name_now.contains("varchar(") 
					&& !type_name_now.contains("float") && !type_name_now.contains("double") 
					&& !type_name_now.contains("datetime") && !type_name_now.contains("date") )
			{	
				System.out.println(type_name_now+"-----Data Type is Invalid! ");
				return false;
			}
			else
				if(type_name_now.contains("char"))
			{
				if(!(type_name_now.contains("(") && type_name_now.contains(")")))
				{
					System.out.println(type_name_now+"-----Data Type is Invalid!\n"
							+ "CHAR and VARCHAR Need to define length.");
					return false;
				}
				if(type_name_now.indexOf('(')+1==type_name_now.indexOf(')'))
				{
					System.out.println(type_name_now+"-----Data Type is Invalid! Please Enter The data Length. ");
					return false;
				}
			}
		}
		
		return whether_right;
	}
	
	
	
}
