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
import java.util.Arrays;
import java.util.List;

/**
 * Primary key meta data
 * @author pranab
 *
 */
public class PrimaryKey {
	private List<String> prmKeyElements;
	private int primKeyElementCount;
	private int rowKeyElementCount = 1;
	private int numPrimKeyComponentsFound;
	private int numPrimKeyComponentsSet;
	
	/**
	 * @param prmKeyElements
	 */
	public PrimaryKey(String... prmKeyElements ) {
		//primary keys could be anywhere in the traversed tree with only one row key component
		this.prmKeyElements = Arrays.asList(prmKeyElements);
	}
	
	/**
	 * @param prmKeyElementsnumPrimKeyComponentsFound
	 * @param rowKeyElementCount
	 */
	public PrimaryKey(List<String> prmKeyElements, int rowKeyElementCount) {
		//primary keys could be anywhere in the traversed tree
		this.prmKeyElements = prmKeyElements;
		this.rowKeyElementCount = rowKeyElementCount;
	}
	
	/**
	 * @param primKeyElementCount
	 * @param rowKeyElementCount
	 */
	public PrimaryKey( int primKeyElementCount, int rowKeyElementCount) {
		//primary keys in the head of the traversed tree
		this.primKeyElementCount = primKeyElementCount;
		this.rowKeyElementCount = rowKeyElementCount;
	}
	
	/**
	 * @param numPrimKeyComponentsSet
	 * @return
	 */
	public PrimaryKey withNumPrimKeyComponentsSet(int numPrimKeyComponentsSet) {
		this.numPrimKeyComponentsSet = numPrimKeyComponentsSet;
		return this;
	}
	
	/**
	 * @param rowKeyElementCount
	 * @return
	 */
	public PrimaryKey withRowKeyElementCount(int rowKeyElementCount) {
		this.rowKeyElementCount = rowKeyElementCount;
		return this;
	}

	/**
	 * @return
	 */
	public List<String> getPrmKeyElements() {
		return prmKeyElements;
	}

	/**
	 * @return
	 */
	public int getPrimKeyElementCount() {
		return primKeyElementCount;
	}

	/**
	 * @return
	 */
	public int getRowKeyElementCount() {
		return rowKeyElementCount;
	}

	/**
	 * @return
	 */
	public int getNumPrimKeyComponentsSet() {
		return numPrimKeyComponentsSet;
	}

	/**
	 * @param rowKeyElementCount
	 */
	public void setRowKeyElementCount(int rowKeyElementCount) {
		this.rowKeyElementCount = rowKeyElementCount;
	}
	
	/**
	 * @return
	 */
	public boolean isOnlyRowKeySet() {
		return numPrimKeyComponentsSet == rowKeyElementCount;
	}

	/**
	 * @return
	 */
	public boolean isFullySet() {
		boolean fullySet = true;
		if (primKeyElementCount > 0) {
			fullySet= numPrimKeyComponentsSet == primKeyElementCount;
		} else {
			fullySet= numPrimKeyComponentsSet == prmKeyElements.size();
		}
		return fullySet;
	}

	/**
	 * Rebuild list with prim key elements at head of list
	 * @param traversedPath
	 * @return
	 */
	public List<NamedObject> movePrimKeyToHead(List<NamedObject> traversedPath) {
		List<NamedObject> newTraversedPath = null;
		numPrimKeyComponentsFound = 0;
		if (primKeyElementCount > 0) {
			//primary key components are at the head of the traversed list 
			newTraversedPath = traversedPath;
			numPrimKeyComponentsFound = primKeyElementCount;
		} else {
			List<NamedObject> head = new ArrayList<NamedObject>(prmKeyElements.size());
			for (int i = 0; i < prmKeyElements.size();  ++i) {
				head.add(null);
			}
			List<NamedObject> tail = new ArrayList<NamedObject>();
			
			//separate primary key and non primary key fields
			for (NamedObject obj :  traversedPath) {
				boolean matched = false;
				for (int i = 0; i < prmKeyElements.size(); ++i) {
					if (prmKeyElements.get(i).equals(obj.getName())) {
						head.set(i, obj);
						matched = true;
						++numPrimKeyComponentsFound;
						break;
					}
				}
				
				if (!matched) {
					//non primary key
					tail.add(obj);
				}
			}
			
			//check if there is gap in primary key components
			for (int i = 0; i  < head.size(); ++i) {
				if (null == head.get(i)) {
					throw new IllegalArgumentException("primary key subset provided is not contiguous");
				}
			}
			
			//make sure all row key component elements are provided
			if (numPrimKeyComponentsFound < rowKeyElementCount) {
				throw new IllegalArgumentException("primary key does not contain all the row key components" +
						"numPrimKeyComponentsFound:" + numPrimKeyComponentsFound + 
						" rowKeyElementCount: " +rowKeyElementCount );
			}
			
			//put them together
			head.addAll(tail);
			newTraversedPath = head;
			primKeyElementCount = prmKeyElements.size();
		}
		return newTraversedPath;
	}
	
	/**
	 * @return
	 */
	public boolean allRowKeyComponentsDefined() {
		return numPrimKeyComponentsFound >= rowKeyElementCount;
	}
	
	/**
	 * @return
	 */
	public boolean allPrimKeyComponentsDefined() {
		return numPrimKeyComponentsFound == primKeyElementCount;
	}

	/**
	 * @return
	 */
	public int getNumPrimKeyComponentsFound() {
		return numPrimKeyComponentsFound;
	}
}
