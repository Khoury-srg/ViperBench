package bench;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Tables {
	public static ObjectMapper om = new ObjectMapper();

	public static HashMap<String, Object> decodeTable(String json) {
		if (json == null) {
			return null;
		}
		try {
			HashMap<String, Object> result = om.readValue(json, HashMap.class);
			return result;
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	public static String encodeTable(HashMap<String, Object> t) {
		try {
			String result = om.writeValueAsString(t);
			return result;
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static String encodeKey(String tableName, String keys[]) {
		String ret = tableName;
		for (int i = 0; i < keys.length; i++) {
			ret += ":" + keys[i];
		}
		return ret;
	}

	public static String encodeKey(String tableName, int keys[]) {
		String ret = tableName;
		for (int i = 0; i < keys.length; i++) {
			ret += ":" + keys[i];
		}
		return ret;
	}

	public static String encodeList(ArrayList<String> l) {
		try {
			String result = om.writeValueAsString(l);
			return result;
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static ArrayList<String> decodeList(String json) {
		if (json == null) {
			return new ArrayList<String>();
		}
		try {
			ArrayList<String> result = om.readValue(json, ArrayList.class);
			return result;
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static Map<String, String> DecodeTableKey(String key, String[] tables) {
		// NOTE: key == <table>###<real key> for Crack, <table>:<real key> for Twitter
		String[] tokens = null;
		Map<String, String> ret = new HashMap<>();
		String tableToken;
		String keyToken;

		if(key == null || !key.contains("###")){
			tableToken = tables[0];
			keyToken = key;
		} else {
			tokens = key.split("###");
			assert tokens.length == 2;
//			tableToken = tokens[0];
			tableToken = tables[0]; // Tricky: now matter which table is specified, we only support this

			if(tokens[1].startsWith("key")){
				keyToken = tokens[1].substring(3);
			} else {
				keyToken = tokens[1];
			}
		}

		ret.put("table", tableToken);
		ret.put("key", keyToken);

		return ret;
	}

}
