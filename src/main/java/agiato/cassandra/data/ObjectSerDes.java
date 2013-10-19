/*
 * Agiato: A simple no frill Cassandra API
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package agiato.cassandra.data;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * Create column values from nested objects and vice versa
 * @author pranab
 *
 */
public class ObjectSerDes {
	private PrimaryKey primKey;
	private List<NamedObject> traversedPath;
	private ByteBuffer rowKey;
	private List<ColumnValue> colValues;
	private boolean partialClusterKey;
	private ByteBuffer clusterKey;

	/*
	 * @param primKey
	 */
	public  ObjectSerDes(PrimaryKey primKey) {
		super();
		this.primKey = primKey;
	}

	/**
	 * @param root
	 * @return
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonParseException 
	 */
	public  List<NamedObject> deconstruct(Object root) throws Exception {
		traversedPath = new ArrayList<NamedObject>();
		if (root instanceof ObjectNode) {
			//object node
			depthFirstTraverse( (ObjectNode)root, new ArrayList<ObjectNode>(), traversedPath);
		} else if (root instanceof SimpleDynaBean) {
			//simple dynamic bean
			depthFirstTraverse(((SimpleDynaBean)root).getMap(), new ArrayList<String>(), traversedPath, false);
		} else if (root instanceof String) {
			//JSON string
			ObjectMapper mapper = new ObjectMapper();
			InputStream is = new ByteArrayInputStream(((String)root).getBytes());
			Map<String, Object> map = mapper.readValue(is, new TypeReference<Map<String, Object>>() {});
			depthFirstTraverse(map, new ArrayList<String>(), traversedPath, true);
		}
		return traversedPath;
	}
	
	
	/**
	 * @param node
	 * @param rootPath
	 * @param traversedPath
	 */
	private void depthFirstTraverse(ObjectNode node, List<ObjectNode> rootPath, List<NamedObject> traversedPath) {
		if (node.hasChildren()) {
			rootPath.add(node);
			for (ObjectNode obj  :  node.getChildren()) {
				depthFirstTraverse(obj,  rootPath,  traversedPath);
			}
			rootPath.remove(rootPath.size() - 1);
		} else {
			String prefix = getNamePrefixObjectNode(rootPath);
			String name = prefix.length() == 0 ? node.getName() : prefix + node.getName();
			NamedObject nObj = new NamedObject(name, node.getValue() );
			traversedPath.add(nObj);
		}
	}
	
	/**
	 * @param rootPath
	 * @return
	 */
	private String getNamePrefixObjectNode(List<ObjectNode> rootPath) {
		boolean first = true;
		String prefix = "";
		for (ObjectNode obj : rootPath) {
			if (first) {
				first = false;
			} else {
				prefix = prefix + obj.getName() + ".";
			}
		}
		return prefix;
	}
	
	/**
	 * @param node
	 * @param rootPath
	 * @param traversedPath
	 * @param realMap
	 */
	private void depthFirstTraverse(Map<String, Object> node, List<String> rootPath, 
			List<NamedObject> traversedPath, boolean realMap) {
		for (String key : node.keySet()) {
			Object obj = node.get(key);
			if (obj instanceof Map<?,?>) {
				addToRootPath(rootPath,  key,  realMap);
				depthFirstTraverse((Map<String, Object>)obj, rootPath,  traversedPath, true);
				rootPath.remove(rootPath.size() - 1);
			} else if (obj instanceof SimpleDynaBean) { 
				addToRootPath(rootPath,  key,  realMap);
				depthFirstTraverse(((SimpleDynaBean)obj).getMap(), rootPath,  traversedPath, false);
				rootPath.remove(rootPath.size() - 1);
			} else if (obj instanceof List<?>) {
				addToRootPath(rootPath,  key,  realMap);
				List<?> listObj  = (List<?>)obj;
				int i = 0;
				for (Object child : listObj) {
					rootPath.add("[" + i + "]");
					if (child instanceof Map<?,?>) {
						depthFirstTraverse((Map<String, Object>)child, rootPath,  traversedPath, true);
					} else if (child  instanceof SimpleDynaBean) { 
						depthFirstTraverse(((SimpleDynaBean)child).getMap(), rootPath,  traversedPath, false);
					} else {
						String name = getNamePrefixMap(rootPath);
						NamedObject nObj = new NamedObject(name, obj );
						traversedPath.add(nObj);
					}
					rootPath.remove(rootPath.size() - 1);
					++i;
				}
				rootPath.remove(rootPath.size() - 1);
			} else {
				String prefix = getNamePrefixMap(rootPath);
				String modKey = realMap ? "{ " + key + "}" : key;
				String name = prefix.length() == 0 ? modKey : prefix + modKey;
				NamedObject nObj = new NamedObject(name, obj );
				traversedPath.add(nObj);
			}
		}
	}

	/**
	 * @param rootPath
	 * @param key
	 * @param realMap
	 */
	private void addToRootPath(List<String> rootPath, String key, boolean realMap) {
		if (realMap) {
			rootPath.add("{" + key + "}");
		} else {
			rootPath.add(key);
		}
	}
	
	/**
	 * @param rootPath
	 * @return
	 */
	private String getNamePrefixMap(List<String> rootPath) {
		boolean first = true;
		String prefix = "";
		for (String path  : rootPath) {
				prefix = prefix + path + ".";
		}
		return prefix;
	}
	
	
	/**
	 * @throws IOException
	 */
	public void serialize() throws IOException {
		colValues = new ArrayList<ColumnValue>();
		
		//serialize column values
		serializeColumnValues();

		//prim  key elements to head of list
		traversedPath = primKey.movePrimKeyToHead(traversedPath);
		
		//row key
		serializeRowKey();
		
		//make sure all prim key components are provided
		if (!primKey.allPrimKeyComponentsDefined()) {
			throw new IllegalArgumentException("all primary key componets not provided");
		}
		
		//column prefix
		List<byte[]> byteArrList = new ArrayList<byte[]>();
		for (int i = primKey.getRowKeyElementCount(); i < primKey.getPrimKeyElementCount(); ++i) {
			byteArrList.add((byte[])traversedPath.get(i).getValue());
		}
		
		//columns
		byte[] bytes = null;
		ColumnValue colValue = null;
		ByteBuffer col; 
		for (int i = primKey.getPrimKeyElementCount(); i < traversedPath.size(); ++i) {
			colValue = new ColumnValue();
			
			//name
			bytes = Util.getBytesFromObject(traversedPath.get(i).getName());
			byteArrList.add(bytes);
			col = ByteBuffer.wrap(Util.encodeComposite(byteArrList));
			colValue.setName(col);
			
			//value 
			col = ByteBuffer.wrap((byte[])traversedPath.get(i).getValue());
			colValue.setValue(col);
			
			colValues.add(colValue);
			byteArrList.remove(bytes);
		}
	}
	
	/**
	 * @throws IOException
	 */
	public void serializePrimKey() throws IOException {
		//serialize column values
		serializeColumnValues();

		//prim  key elements to head of list
		traversedPath = primKey.movePrimKeyToHead(traversedPath);
		
		//row key
		serializeRowKey();

		//cluster key
		List<byte[]> byteArrList = new ArrayList<byte[]>();
		for (int i = primKey.getRowKeyElementCount(); i < primKey.getNumPrimKeyComponentsFound(); ++i) {
			byteArrList.add((byte[])traversedPath.get(i).getValue());
		}
		clusterKey = ByteBuffer.wrap(Util.encodeComposite(byteArrList));
	}

	/**
	 * return column key range corresponding to cluster key
	 * @return
	 */
	public List<ByteBuffer> getColumnRange() {
		List<ByteBuffer> colRange = new ArrayList<ByteBuffer>();
		
		byte[] startBytes  = new byte[clusterKey.remaining()];
		clusterKey.get(startBytes);
		byte[] endBytes = Arrays.copyOf(startBytes, startBytes.length);
		endBytes[endBytes.length-1] = 1;
		ByteBuffer endBuffer = ByteBuffer.wrap(endBytes);
		colRange.add(clusterKey);
		colRange.add(endBuffer);
		
		return colRange;
	}
	
	/**
	 * @throws IOException
	 */
	private void serializeColumnValues() throws IOException {
		byte[] bytes = null;
		for (NamedObject obj : traversedPath) {
			Object val = obj.getValue();
			if (null != val) {
				bytes =  Util.getBytesFromObject(val);
				obj.setValue(bytes);
			}
		}
	}
	
	/**
	 * serialize row key 
	 */
	private void serializeRowKey() {
		List<byte[]> byteArrList = new ArrayList<byte[]>();
		for (int i =0; i < primKey.getRowKeyElementCount(); ++i) {
			byteArrList.add((byte[])traversedPath.get(i).getValue());
		}
		rowKey = ByteBuffer.wrap(Util.encodeComposite(byteArrList));
	}

	/**
	 * 
	 */
	private void serializeClusterKey() {
		partialClusterKey = false;
		List<byte[]> byteArrList = new ArrayList<byte[]>();
		for (int i = primKey.getRowKeyElementCount(); i < primKey.getPrimKeyElementCount(); ++i) {
			if (null != traversedPath.get(i).getValue()) {
				byteArrList.add((byte[])traversedPath.get(i).getValue());
			} else {
				partialClusterKey = true;
				break;
			}
		}
		
	}
	
	
	/**
	 * @return
	 */
	public List<NamedObject> getTraversedPath() {
		return traversedPath;
	}

	/**
	 * @param traversedPath
	 */
	public void setTraversedPath(List<NamedObject> traversedPath) {
		this.traversedPath = traversedPath;
	}

	/**
	 * @return
	 */
	public ByteBuffer getRowKey() {
		return rowKey;
	}

	/**
	 * @param rowKey
	 */
	public void setRowKey(ByteBuffer rowKey) {
		this.rowKey = rowKey;
	}

	/**
	 * @return
	 */
	public List<ColumnValue> getColValues() {
		return colValues;
	}

	/**
	 * @param colValues
	 */
	public void setColValues(List<ColumnValue> colValues) {
		this.colValues = colValues;
	}

}
