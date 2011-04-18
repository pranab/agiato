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

package agiato.cassandra.meta;

/**
 *
 * @author pranab
 */
public class ColumnFamilyDef {
    private String name;
    private String columnType;
    private String compareWith;
    private String compareSubcolumnsWith;
    private int keysCached;
    private int rowsCached;

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the columnType
     */
    public String getColumnType() {
        return columnType;
    }

    /**
     * @param columnType the columnType to set
     */
    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    /**
     * @return the compareWith
     */
    public String getCompareWith() {
        return compareWith;
    }

    /**
     * @param compareWith the compareWith to set
     */
    public void setCompareWith(String compareWith) {
        this.compareWith = compareWith;
    }

    /**
     * @return the compareSubcolumnsWith
     */
    public String getCompareSubcolumnsWith() {
        return compareSubcolumnsWith;
    }

    /**
     * @param compareSubcolumnsWith the compareSubcolumnsWith to set
     */
    public void setCompareSubcolumnsWith(String compareSubcolumnsWith) {
        this.compareSubcolumnsWith = compareSubcolumnsWith;
    }

    /**
     * @return the keysCached
     */
    public int getKeysCached() {
        return keysCached;
    }

    /**
     * @param keysCached the keysCached to set
     */
    public void setKeysCached(int keysCached) {
        this.keysCached = keysCached;
    }

    /**
     * @return the rowsCached
     */
    public int getRowsCached() {
        return rowsCached;
    }

    /**
     * @param rowsCached the rowsCached to set
     */
    public void setRowsCached(int rowsCached) {
        this.rowsCached = rowsCached;
    }
    
    public boolean isSuperColumn(){
        return columnType.equals("Super");
    }
    
    public boolean isKeysCachedSet(){
        return keysCached > 0;
    } 

    public boolean isRowsCachedSet(){
        return rowsCached > 0;
    } 
}
