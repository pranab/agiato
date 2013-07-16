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

/**
 * Primary key
 * @author pranab
 *
 */
public class PrimaryKey {
	private List<String> prmKeyElements;
	private int primKeyElementCount;
	private int rowKeyElementCount;
	
	public PrimaryKey(List<String> prmKeyElements, int rowKeyElementCount) {
		super();
		this.prmKeyElements = prmKeyElements;
		this.rowKeyElementCount = rowKeyElementCount;
	}
	
	public PrimaryKey( int primKeyElementCount, int rowKeyElementCount) {
		super();
		this.primKeyElementCount = primKeyElementCount;
		this.rowKeyElementCount = rowKeyElementCount;
	}
	
	public List<String> getPrmKeyElements() {
		return prmKeyElements;
	}

	public int getPrimKeyElementCount() {
		return primKeyElementCount;
	}

	public int getRowKeyElementCount() {
		return rowKeyElementCount;
	}

	/**
	 * Rebuild list with prim key elements at head of list
	 * @param traversedPath
	 * @return
	 */
	public List<NamedObject> movePrimKeyToHead(List<NamedObject> traversedPath) {
		List<NamedObject> newTraversedPath = null;
		if (primKeyElementCount > 0) {
			newTraversedPath = traversedPath;
		} else {
			List<NamedObject> head = new ArrayList<NamedObject>(prmKeyElements.size());
			List<NamedObject> tail = new ArrayList<NamedObject>();
			
			//separate prim and non prim
			for (NamedObject obj :  traversedPath) {
				boolean matched = false;
				for (int i = 0; i < prmKeyElements.size(); ++i) {
					if (prmKeyElements.get(i).equals(obj.getName())) {
						head.set(i, obj);
						matched = true;
						break;
					}
				}
				
				if (!matched) {
					tail.add(obj);
				}
			}
			
			//put them together
			head.addAll(tail);
			newTraversedPath = head;
			primKeyElementCount = prmKeyElements.size();
		}
		return newTraversedPath;
	}
}
