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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Create column values from nested objects and vice versa
 * @author pranab
 *
 */
public class ObjectSerDes {
	private int rowKeyCompCount;
	private int primKeyCompnentCount;
	private List<NamedObject> traversedPath;
	private ByteBuffer rowKey;
	private List<ColumnValue> colValues;

	/**
	 * @param rowKeyCompCount
	 * @param primKeyCompnentCount
	 */
	public ObjectSerDes(int rowKeyCompCount, int primKeyCompnentCount) {
		super();
		this.rowKeyCompCount = rowKeyCompCount;
		this.primKeyCompnentCount = primKeyCompnentCount;
	}

	/**
	 * @param root
	 * @return
	 */
	public  List<NamedObject> deconstruct(ObjectNode root) {
		traversedPath = new ArrayList<NamedObject>();
		depthFirstTraverse( root, new ArrayList<ObjectNode>(), traversedPath);
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
			String prefix = getNamePrefix(rootPath);
			String name = prefix.length() == 0 ? node.getName() : prefix + node.getName();
			NamedObject nObj = new NamedObject(name, node.getValue() );
			traversedPath.add(nObj);
		}
	}
	
	/**
	 * @param rootPath
	 * @return
	 */
	private String getNamePrefix(List<ObjectNode> rootPath) {
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
	 * @throws IOException
	 */
	public void serialize() throws IOException {
		colValues = new ArrayList<ColumnValue>();
		
		//column values
		byte[] bytes = null;
		for (NamedObject obj : traversedPath) {
			Object val = obj.getValue();
			bytes = Util.getBytesFromObject(val);
			obj.setValue(bytes);
		}
		
		//row key
		List<byte[]> byteArrList = new ArrayList<byte[]>();
		for (int i =0; i < rowKeyCompCount; ++i) {
			byteArrList.add((byte[])traversedPath.get(i).getValue());
		}
		rowKey = ByteBuffer.wrap(Util.makeComposite(byteArrList));

		//column prefix
		byteArrList.clear();
		for (int i = rowKeyCompCount; i < primKeyCompnentCount; ++i) {
			byteArrList.add((byte[])traversedPath.get(i).getValue());
		}
		
		//columns
		ColumnValue colValue = null;
		ByteBuffer col; 
		for (int i = primKeyCompnentCount; i < traversedPath.size(); ++i) {
			colValue = new ColumnValue();
			
			//name
			bytes = Util.getBytesFromObject(traversedPath.get(i).getName());
			byteArrList.add(bytes);
			col = ByteBuffer.wrap(Util.serializeComposite(byteArrList));
			colValue.setName(col);
			
			//value 
			col = ByteBuffer.wrap((byte[])traversedPath.get(i).getValue());
			colValue.setValue(col);
			
			colValues.add(colValue);
			byteArrList.remove(bytes);
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
