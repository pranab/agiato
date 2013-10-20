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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Dynamic object
 * @author pranab
 *
 */
public class ObjectNode extends NamedObject {
	private List<ObjectNode> children = new ArrayList<ObjectNode>();

	public ObjectNode(String name) {
		super(name);
	}

	public ObjectNode(String name, Object value) {
		super(name,  value);
	}
	
	public void addChild(ObjectNode child) {
		children.add(child);
	}

	public void addListChild(List<ObjectNode> listChild) {
		int i = 0;
		ObjectNode parent;
		for (ObjectNode child : listChild) {
			parent = new ObjectNode("[" + i + "]");
			addChild(parent);
			parent.addChild(child);
			++i;
		}
	}

	public void addListChild(ObjectNode child, int index) {
		int i = 0;
		ObjectNode parent  = new ObjectNode("[" + index + "]");
		addChild(parent);
		parent.addChild(child);
	}

	
	public void addMapChild(Map<String, ObjectNode> mapChild) {
		ObjectNode parent;
		for (String key : mapChild.keySet()) {
			parent = new ObjectNode("{" + key + "}");
			addChild(parent);
			parent.addChild(mapChild.get(key));
		}
	}

	public boolean hasChildren() {
		return !children.isEmpty();
	}

	public List<ObjectNode> getChildren() {
		return children;
	}

}
