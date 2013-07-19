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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.DynaClass;

/**
 * Simple dynamic bean
 * @author pranab
 *
 */
public class SimpleDynaBean  implements DynaBean {
	private Map<String, Object> map = new HashMap<String, Object>();

	/* (non-Javadoc)
	 * @see org.apache.commons.beanutils.DynaBean#contains(java.lang.String, java.lang.String)
	 */
	public boolean contains(String name,  String key) {
		return map.containsKey(key);
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.beanutils.DynaBean#get(java.lang.String)
	 */
	public Object get(String name) {
		return map.get(name);
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.beanutils.DynaBean#get(java.lang.String, int)
	 */
	public Object get(String name, int index) {
		Object value = null;
		List<Object> chList = (List<Object>)map.get(name);
		if (null != chList && index < chList.size()) {
			value = chList.get(index);
		}
		return value;
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.beanutils.DynaBean#get(java.lang.String, java.lang.String)
	 */
	public Object get(String name, String key) {
		Object value = null;
		Map<String, Object> chMap = (Map<String, Object>)map.get(name);
		if (null != chMap) {
			value = chMap.get(key);
		}
		return value;
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.beanutils.DynaBean#getDynaClass()
	 */
	public DynaClass getDynaClass() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.beanutils.DynaBean#remove(java.lang.String, java.lang.String)
	 */
	public void remove(String name, String key) {
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.beanutils.DynaBean#set(java.lang.String, java.lang.Object)
	 */
	public void set(String name, Object value) {
		map.put(name, value);
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.beanutils.DynaBean#set(java.lang.String, int, java.lang.Object)
	 */
	public void set(String name, int index, Object value) {
		List<Object> chList = (List<Object>)map.get(name);
		if (null == chList) {
			chList = new ArrayList<Object>();
			map.put(name, chList);
		}
		chList.add(index, value);
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.beanutils.DynaBean#set(java.lang.String, java.lang.String, java.lang.Object)
	 */
	public void set(String name, String key, Object value) {
		Map<String, Object> chMap = (Map<String, Object>)map.get(name);
		if (null == chMap) {
			chMap = new HashMap<String, Object>();
			map.put(name, chMap);
		}
		chMap.put(key, value);
	}
	
	/**
	 * @return
	 */
	public Map<String, Object> getMap() {
		return map;
	}

}
