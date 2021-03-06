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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
/**
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
	private Map<List<BigInteger>, Object> dataObjects = new HashMap<List<BigInteger>, Object>();
	private List<byte[]> rowKeyByteArrList = new ArrayList<byte[]>();
	private List<byte[]> colNameComponents;
	private String   nonPrimKeyColName;
	private List<BigInteger>  clutserKey;
	private Map<String, Object> nodeValueMap = new  HashMap<String, Object>();
	private boolean rawNodeValue ;
	
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
				//map
				addToRootPath(rootPath,  key,  realMap);
				depthFirstTraverse((Map<String, Object>)obj, rootPath,  traversedPath, true);
				rootPath.remove(rootPath.size() - 1);
			} else if (obj instanceof SimpleDynaBean) { 
				//dyna bean
				addToRootPath(rootPath,  key,  realMap);
				depthFirstTraverse(((SimpleDynaBean)obj).getMap(), rootPath,  traversedPath, false);
				rootPath.remove(rootPath.size() - 1);
			} else if (obj instanceof List<?>) {
				//list
				addToRootPath(rootPath,  key,  realMap);
				List<?> listObj  = (List<?>)obj;
				int i = 0;
				
				//traverse list elements
				for (Object child : listObj) {
					rootPath.add("[" + i + "]");
					if (child instanceof Map<?,?>) {
						//map
						depthFirstTraverse((Map<String, Object>)child, rootPath,  traversedPath, true);
					} else if (child  instanceof SimpleDynaBean) { 
						//dyna bean
						depthFirstTraverse(((SimpleDynaBean)child).getMap(), rootPath,  traversedPath, false);
					} else {
						//primitive
						String name = getNamePrefixMap(rootPath);
						NamedObject nObj = new NamedObject(name, obj );
						traversedPath.add(nObj);
					}
					rootPath.remove(rootPath.size() - 1);
					++i;
				}
				rootPath.remove(rootPath.size() - 1);
			} else {
				//primitive
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
		for (int i = primKey.getRowKeyElementCount(); i < primKey.getNumPrimKeyComponentsSet(); ++i) {
			byteArrList.add((byte[])traversedPath.get(i).getValue());
		}
		
		//System.out.println("num cluster key ements:" +byteArrList.size() );
		clusterKey = byteArrList.isEmpty() ? null  :   ByteBuffer.wrap(Util.encodeCompositeAlways(byteArrList));
	}

	/**
	 * @return true if cluster key set
	 */
	public boolean isClusterKeySet() {
		return null != clusterKey;
	}
	
	/**
	 * return column key range corresponding to cluster key
	 * @return
	 */
	public List<ByteBuffer> getColumnRange()  {
		if (null == clusterKey) {
			throw new IllegalStateException("cluster key not set");
		}
		
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
	 * serializes all column values in traversed path
	 * @throws IOException
	 */
	private void serializeColumnValues() throws IOException {
		byte[] bytes = null;
		for (NamedObject obj : traversedPath) {
			Object val = obj.getValue();
			if (null != val) {
				//cache the typed values
				nodeValueMap.put(obj.getName(), val);
				
				//replace with serialized value
				bytes =  Util.getBytesFromObject(val);
				obj.setValue(bytes);
			}
		}
	}
	
	/**
	 * serialize row key 
	 */
	private void serializeRowKey() {
		rowKeyByteArrList.clear();
		for (int i =0; i < primKey.getRowKeyElementCount(); ++i) {
			rowKeyByteArrList.add((byte[])traversedPath.get(i).getValue());
		}
		rowKey = ByteBuffer.wrap(Util.encodeComposite(rowKeyByteArrList));
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

	/**
	 * @param proto
	 * @param colValues
	 * @return
	 * @throws IOException 
	 */
	public List<Object> construct(Object proto, List<ColumnValue> colValues) throws IOException {
		List<Object> values = new ArrayList<Object>();
		Object protoValue = null;
		rawNodeValue = false;
		if (proto instanceof ObjectNode)  {
			//ObjectNode
			for (ColumnValue colVal :  colValues) {
				//cluster key
				createClusterKey(colVal);
				
				//data object for this cluster key
				ObjectNode dataObj  = (ObjectNode)dataObjects.get(clutserKey);
				if (null == dataObj) {
					//create and initialize 
					dataObj = new  ObjectNode("");
					
					//populate row key values
					for (int i =0; i < primKey.getRowKeyElementCount(); ++i) {
						String rowKeyName = primKey.getPrmKeyElements().get(i);
						protoValue = nodeValueMap.get(rowKeyName);
						List<String> rowKeyComponents =getKeyComponents(rowKeyName);
						buildNestedObjectNode(dataObj, rowKeyComponents,  0, rowKeyByteArrList.get(i), protoValue);
					}
					
					//populate cluster key values
					for (int i =  primKey.getRowKeyElementCount(), j = 0; i < primKey.getPrimKeyElementCount() ;++i, ++j) {
						String clusterKeyName = primKey.getPrmKeyElements().get(i);
						protoValue = nodeValueMap.get(clusterKeyName);
						List<String> clusterKeyComponents =getKeyComponents(clusterKeyName);
						buildNestedObjectNode(dataObj, clusterKeyComponents, 0,  colNameComponents.get( j ), protoValue);
					}
					
					//cache it
					dataObjects.put(clutserKey, dataObj);
				}
				
				//populate col value
				List<String> colKeyComponents =getKeyComponents(nonPrimKeyColName);
				protoValue = nodeValueMap.get(nonPrimKeyColName);
				buildNestedObjectNode(dataObj, colKeyComponents,  0, Util.getBytesFromByteBuffer(colVal.getValue()), protoValue);
			}
			
			//collect all the objects
			for (List<BigInteger> primKey : dataObjects.keySet()) {
				values.add(dataObjects.get(primKey));
			}
		} else   {
			//SimpleDynaBean or JSON
			for (ColumnValue colVal :  colValues) {
				//cluster key
				createClusterKey(colVal);
				
				//data object for this cluster key
				Map<String, Object> dataObj  = (Map<String, Object>)dataObjects.get(clutserKey);
				if (null == dataObj) {
					//create and initialize 
					dataObj = new HashMap<String, Object>();
					
					//populate row key values
					for (int i =0; i < primKey.getRowKeyElementCount(); ++i) {
						String rowKeyName = primKey.getPrmKeyElements().get(i);
						List<String> rowKeyComponents =getKeyComponents(rowKeyName);
						protoValue = nodeValueMap.get(rowKeyName);
						buildNestedMap(dataObj, rowKeyComponents,  rowKeyByteArrList.get(i),0, protoValue);
					}
					
					//populate cluster key values
					for (int i =  primKey.getRowKeyElementCount(), j = 0; i < primKey.getPrimKeyElementCount() ;++i, ++j) {
						String clusterKeyName = primKey.getPrmKeyElements().get(i);
						List<String> clusterKeyComponents =getKeyComponents(clusterKeyName);
						protoValue = nodeValueMap.get(clusterKeyName);
						buildNestedMap(dataObj, clusterKeyComponents,  colNameComponents.get( j ),0, protoValue);
					}
					//cache it
					dataObjects.put(clutserKey, dataObj);
				}
				
				//populate col value
				List<String> colKeyComponents =getKeyComponents(nonPrimKeyColName);
				protoValue = nodeValueMap.get(nonPrimKeyColName);
				buildNestedMap(dataObj, colKeyComponents,  Util.getBytesFromByteBuffer(colVal.getValue()),0, protoValue);
			}
			
			if (proto instanceof SimpleDynaBean) {
				//collect all the SimpleDynaBean objects
				for (List<BigInteger> primKey : dataObjects.keySet()) {
					Map<String, Object> map = (Map<String, Object>)dataObjects.get(primKey);
					values.add(new SimpleDynaBean(map));
				}
			} else {
				//collect all the JSON string objects
				for (List<BigInteger> primKey : dataObjects.keySet()) {
					Map<String, Object> map = (Map<String, Object>)dataObjects.get(primKey);
					if (rawNodeValue) {
						//return map, some node values untyped
						values.add(map);
					} else {
						//all node values strongly typed
						ObjectMapper mapper = new ObjectMapper();
						String json = mapper.writeValueAsString(map);
						values.add(json);
					}
				}
			}
		}
		
		return values;
	}
	
	/**
	 * decodes cluster key components and normal column  names
	 * @param colVal
	 * @throws IOException
	 */
	private void createClusterKey(ColumnValue colVal) throws IOException {
		//cluster key
		ByteBuffer colName = colVal.getName();
		colNameComponents = Util.dcodeComposite(Util.getBytesFromByteBuffer(colName));
		nonPrimKeyColName = Util.getStringFromBytes(colNameComponents.remove(colNameComponents.size()-1));
		//System.out.println("nonPrimKeyColName:" + nonPrimKeyColName);
		
		//data object for this cluster key
		clutserKey = toBigIntList(colNameComponents);
	}
	
	
	/**
	 * @param keyName
	 * @return
	 */
	private List<String> getKeyComponents(String keyName) {
		String[] items = keyName.split("\\.");
		return  Arrays.asList(items);
	}
	
	/**
	 * Recursively build ObjectNode graph
	 * @param parent
	 * @param path
	 * @param value
	 * @param protoValue
	 * @throws IOException 
	 */
	private void buildNestedObjectNode(ObjectNode parent, List<String> path, int index, byte[] value, Object protoValue) 
		throws IOException {
		if (null == protoValue) {
			rawNodeValue = true;
		}
		String pathElem = path.get(index);
		ObjectNode child = null;
		if (index == path.size() - 1) {
			//leaf
			Object typedValue = null != protoValue ? Util.getObjectFromBytes(value, protoValue) : value;
			child = new ObjectNode(pathElem, typedValue);
		} else {
			//intermediate
			child = new ObjectNode(pathElem);
		}
		parent.addChild(child);

		if (index < path.size() - 1) {
			//recurse
			++index;
			buildNestedObjectNode(child,  path, index,  value, protoValue);
		}
	}

	/**
	 * Recursively build ObjectNode graph
	 * @param parent
	 * @param path
	 * @param value
	 * @throws IOException 
	 */
	private void buildNestedMap(Map<String, Object> parent, List<String> path, byte[] value, int pathIndex, 
		Object protoValue) throws IOException {
		if (null == protoValue) {
			rawNodeValue = true;
		}
		String pathElem = path.get(pathIndex);
		boolean done =false;
		Object child = null;
		if (pathIndex == path.size() - 1) {
			//end of path
			Object typedValue = null != protoValue ? Util.getObjectFromBytes(value, protoValue) : value;
			parent.put(pathElem, typedValue);
			done = true;
		} else {
			child = parent.get(pathElem);
			String nextPathElem = path.get(pathIndex+1);
			if(nextPathElem.startsWith("[")) {
				//list child
				List<Object> listChild = null;
				if (null == child) {
					//list child does not exist
					listChild = new ArrayList<Object>();
					parent.put(pathElem, listChild);
				} else {
					//list child exists
					listChild = (List<Object>)child;
				}
				
				//insert list element
				int listIndex = Integer.parseInt(nextPathElem.substring(1, nextPathElem.length()-1));
				if(pathIndex == path.size() - 2) {
					//atomic value
					listChild.add(listIndex, value);
					done = true;
				} else {
					child = new HashMap<String, Object>();
					listChild.add(listIndex, child);
					pathIndex += 2;
				}
			} if(nextPathElem.startsWith("{")) {
				//map child
				Map<String, Object> mapChild = null;
				if (null == child) {
					//map child does not exist
					mapChild = new HashMap<String,Object>();
					parent.put(pathElem, mapChild);
				} else {
					//map child exists
					mapChild = (Map<String,Object>)child;
				}
				
				//insert map element
				String mapKey = nextPathElem.substring(1, nextPathElem.length()-1);
				if(pathIndex == path.size() - 2) {
					//atomic value
					mapChild.put(mapKey, value);
					done = true;
				} else {
					child = new HashMap<String, Object>();
					mapChild.put(mapKey, child);
					pathIndex += 2;
				}
			} else {
				//other object
				if (null == child) {
					child = new HashMap<String, Object>();
					parent.put(pathElem, child);
				}
				++pathIndex;
			}
		}
		
		if (!done) {
			//recursive call
			buildNestedMap((Map<String, Object>)child,  path,  value, pathIndex,protoValue);
		}
	}
	
	/**
	 * @param colNameComponents
	 * @return
	 */
	private List<BigInteger> toBigIntList(List<byte[]> colNameComponents) {
		List<BigInteger> bigIntList = new ArrayList<BigInteger>();
		for (byte[] item :  colNameComponents) {
			bigIntList.add(new BigInteger(item));
		}
		return bigIntList;
	}
}
