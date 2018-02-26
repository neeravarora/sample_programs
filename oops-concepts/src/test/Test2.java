package test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test2 {
	
	
	List<String> tableHeaders = new ArrayList<String>();
	
	static String input = "           OSPF Router with ID (10.106.69.21) (Process ID 1)\n"+
"\n"+ 
"              Router Link States (Area 0)\n"+
"\n"+ 
"Link ID         ADV Router      Age         Seq#       Checksum Link count\n"+
"4.4.4.4         4.4.4.4         467         0x8000048E 0x00659D 3\n"+
"10.106.69.21    10.106.69.21    946         0x80000384 0x009C8D 1\n"+
"\n"+ 
"              Net Link States (Area 0)\n"+
"\n"+ 
"Link ID         ADV Router      Age         Seq#       Checksum\n"+
"110.1.1.2       10.106.69.21    946         0x80000381 0x006234";

	
	
	
	
	public Test2() {
		tableHeaders.add("Router Link States");
		tableHeaders.add("Net Link States");
	}

	public static String getTableSplitRegex(String tableName) {
		StringBuilder regex = new StringBuilder(tableName);
		regex.append("*");
        return regex.toString();
	}

String table = "Link ID         ADV Router      Age         Seq#       Checksum Link count\n"+
		       "4.4.4.4         4.4.4.4         467         0x8000048E 0x00659D 3\n"+
		       "10.106.69.21    10.106.69.21    946         0x80000384 0x009C8D 1\n";

	public static void main(String[] args) {
		String[] lines = input.split("/n");
		Arrays.stream(lines);
		
		System.out.println(input.matches("Router Link States"));
		System.out.println(input);

	}

}
