import java.util.ArrayList;


public class CommandParser {

	public static String parseUse(String command_string){
		String [] str=command_string.split("\\s+");
		String string_use=str[1];
		return string_use;
	}

	public static String parseCreateSchema(String command_string){
		String [] str=command_string.split("\\s+");
		String string_createschema=str[2];
		return string_createschema;
	}

	public static String parseCreateTableTName(String command_string){
		String command_string_begin=command_string.substring(0,command_string.indexOf('('));
		String [] str=command_string_begin.split("\\s+");
		String string_tablename=str[2];
		return string_tablename;
	}
	

	public static ArrayList<String> parseCreateTableColumnNames(String command_string){
		String str=command_string.substring(command_string.indexOf('(')+1, command_string.length()-1);
		String [] string_constraint = str.split(",");
		ArrayList<String> string_columnnames=new ArrayList<>();
		int i = 0;
		while(i < string_constraint.length)
		{
			String string_name_type = string_constraint[i].substring(0, string_constraint[i].indexOf('[')-1);
			if (Character.isWhitespace(string_name_type.charAt(0)))
				string_constraint[i]=string_name_type.replaceFirst("\\s+","");
			else 
				string_constraint[i]=string_name_type;
			String [] string_name_type_array=string_constraint[i].split("\\s+");
			String string_name=string_name_type_array[0];
			string_columnnames.add(string_name);
			i++;
		}
		return string_columnnames;
	}
	

	public static ArrayList<String> parseCreateTableDataTypes(String command_string){
		String str=command_string.substring(command_string.indexOf('(')+1, command_string.length()-1);
		String [] string_constraint = str.split(",");
		ArrayList<String> string_columnnames=new ArrayList<>();
		
		int i = 0;
		while (i < string_constraint.length)
		{
			String Type="";
			String string_name_type = string_constraint[i].substring(0, string_constraint[i].indexOf('[')-1);
			if (Character.isWhitespace(string_name_type.charAt(0)))
				string_constraint[i]=string_name_type.replaceFirst("\\s+","");
			else 
				string_constraint[i]=string_name_type;
			String [] eachName_and_Type=string_constraint[i].split("\\s+");
			
			for(int j=1; j<eachName_and_Type.length;j++){
			Type = Type + eachName_and_Type[j];}
			string_columnnames.add(Type);
			
			i++;
		}
		return string_columnnames;
	}
	

	public static ArrayList<String> parseCreateTableKey(String command_string){
		ArrayList<String> table_keys=new ArrayList<>();
		String str = command_string.substring(command_string.indexOf('(')+1, command_string.length()-1);
		String [] string_constraint = str.split(",");
		
		int i = 0;
		while (i < string_constraint.length)
		{
			String table_key_moment=string_constraint[i].substring(string_constraint[i].indexOf('[')+1, string_constraint[i].indexOf('|'));
			if(string_constraint[i].indexOf('[')+1==string_constraint[i].indexOf('|'))				
			    table_keys.add("");
			else 
				table_keys.add(table_key_moment);
			
			i++;
		}
		return table_keys;
	}
	
	

	public static ArrayList<String> parseCreateTableNull(String command_string){
		ArrayList<String> null_status=new ArrayList<>();
		String str=command_string.substring(command_string.indexOf('(')+1, command_string.length()-1);
		String [] string_constraint = str.split(",");
		int i = 0;
		while (i < string_constraint.length)
		{
			String NullStatus=string_constraint[i].substring(string_constraint[i].indexOf('|')+1, string_constraint[i].indexOf(']'));
			if(string_constraint[i].indexOf('|')+1==string_constraint[i].indexOf(']'))
			    null_status.add("");
			else
				null_status.add(NullStatus);
			
			i++;
		}
		return null_status;
	}

	public static String parseInsertTableName(String command_string){
		String table_name="";
		String str=command_string.substring(0,command_string.indexOf('('));
		String [] result=str.split("\\s+");
		table_name=result[2];
		return table_name;
	}


	public static ArrayList<String> parseInsertValues(String command_string){
		ArrayList<String> inserted_values_list=new ArrayList<>();
		String inserted_values=command_string.substring(command_string.indexOf('(')+1,command_string.indexOf(')'));
		
		String values1=inserted_values.replaceAll("\'", "");
		String [] result=values1.split(",");
		for(int i=0; i<result.length;i++)
		{inserted_values_list.add(result[i].replaceAll("\\s+", ""));}
		return inserted_values_list;
	}
	

	public static String parseSelectTableName(String command_string){
		String [] result=command_string.split("\\s+");
		return result[3];
	}
	

	public static String parseSelectColumnName(String command_string){
		String [] result=command_string.split("\\s+");
		String column_name = result[5].substring(0, parseSelectComparisonPosition1(result[5]));
		return column_name;
	}
	
	public static int parseSelectComparison(String command_string){
		if(command_string.contains("="))
			return 1;
		else if(command_string.contains("<>"))
			return 2;
		else if(command_string.contains("<") && !command_string.contains(">"))
			return 3;
		else if(command_string.contains(">") && !command_string.contains("<"))
			return 4;
		else 
			return 0;
	}
	
	public static String parseSelectValue(String command_string){
		String result=command_string.substring(parseSelectComparisonPosition(command_string)+1,command_string.length());
		String result1=result.replaceAll("'","");
		String result2=result1.replaceAll("\\s+", "");
		return result2;
	}
	
	public static int parseSelectComparisonPosition(String command_string){
		if(command_string.contains("="))
			return command_string.indexOf('=');
		else if(command_string.contains("<>"))
			return command_string.indexOf('>');
		else if(command_string.contains("<") && !command_string.contains(">"))
			return command_string.indexOf('<');
		else if(command_string.contains(">") && !command_string.contains("<"))
			return command_string.indexOf('>');
		else 
			return command_string.length();
	}
	
	public static int parseSelectComparisonPosition1(String command_string){
		if(command_string.contains("="))
			return command_string.indexOf('=');
		else if(command_string.contains("<>"))
			return command_string.indexOf('<');
		else if(command_string.contains("<") && !command_string.contains(">"))
			return command_string.indexOf('<');
		else if(command_string.contains(">") && !command_string.contains("<"))
			return command_string.indexOf('>');
		else 
			return command_string.length();
	}
	
public static String parseDropTableName(String command_string){
	String [] result=command_string.split("\\s+");
	return result[2];
}


public static int myParser(String command_string_begin){
	String command_string=command_string_begin.toLowerCase();
	if(command_string.startsWith("show schemas"))
		return 1;
	else 
		if(command_string.startsWith("show tables"))
		return 2;
	else 
		if(command_string.startsWith("use "))
		return 3;
	else
		if(command_string.startsWith("create schema "))
		return 4;
	else
		if(command_string.startsWith("create table "))
		return 5;
	else
		if(command_string.startsWith("insert into "))
		return 6;
	else
		if(command_string.startsWith("select *"))
		return 7;
	else 
		if(command_string.startsWith("show columns"))
		return 8;
	else 
		if(command_string.startsWith("help"))
		return 9;
	else 
		if(command_string.startsWith("drop table "))
		return 10;
	else return 0;
}



}
