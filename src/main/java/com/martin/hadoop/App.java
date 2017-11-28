package com.martin.hadoop;

import net.sf.json.JSONObject;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {	
    	String data = "{'Code':3227910192,'Message':分配产品授权给节点失败,'Data':[{'err_id': 3227910192L, 'exp_msg': '\\u5206\\u914d\\u4ea7\\u54c1\\u6388\\u6743\\u7ed9\\u8282\\u70b9\\u5931\\u8d25', 'time': '2017-11-20 15:23:50'}]}";
//    	data = data.replaceAll("'", "\"");
    	int endIndex = data.indexOf(",");
		if (endIndex==-1) {
			return;
		}
		data = data.substring(0, endIndex)+"}";
    	System.out.println(data);
    	JSONObject object = JSONObject.fromObject(data);
    	System.out.println(object.getString("Code"));;
        System.out.println( "Hello World!" );
    }
}
