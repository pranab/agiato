/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package agiato.cassandra.meta;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author pranab
 */
public class IndexDef {

    public IndexDef() {
        
    }
    
    private String name;
    private String colFamily;
    private String type;
    private String indexedColumnName;
    private String cachedColumnName;
    private int indexedDataType;
    private int storedDataType;
    private int indexKeysCached;
    private int indexRowsCached;
        
    private String colFamilyName;
    private String[] typeItems;
    
    private static Map<String, String> catMap = new HashMap<String, String>();
    private static Map<String, String> structureMap = new HashMap<String, String>();
    private static Map<String, String> indexedDataTypeMap = new HashMap<String, String>();
    private static Map<String, String> storedDataTypeMap = new HashMap<String, String>();
    
    static {
        catMap.put("1", "Name");
        catMap.put("1", "Val");
        
        structureMap.put("1", "Rk");
        structureMap.put("2", "Rk");
        structureMap.put("3", "Sc");
        
        indexedDataTypeMap.put("1", "String");
        indexedDataTypeMap.put("2", "Long");
        
        storedDataTypeMap.put("1", "String");
        storedDataTypeMap.put("2", "Long");
    }
    
    public static final int TYPE_STRING = 1; 
    public static final int TYPE_LONG = 2; 

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
     * @return the colFamily
     */
    public String getColFamily() {
        return colFamily;
    }

    /**
     * @param colFamily the colFamily to set
     */
    public void setColFamily(String colFamily) {
        this.colFamily = colFamily;
    }

    /**
     * @return the indexedColumn
     */
    public String getIndexedColumnName() {
        return indexedColumnName;
    }

    /**
     * @param indexedColumn the indexedColumn to set
     */
    public void setIndexedColumnName(String indexedColumnName) {
        this.indexedColumnName = indexedColumnName;
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
    }


    /**
     * @return the indexedDataType
     */
    public int getIndexedDataType() {
        return indexedDataType;
    }

    /**
     * @param indexedDataType the indexedDataType to set
     */
    public void setIndexedDataType(int indexedDataType) {
        this.indexedDataType = indexedDataType;
    }

    /**
     * @return the storedDataType
     */
    public int getStoredDataType() {
        return storedDataType;
    }

    /**
     * @param storedDataType the storedDataType to set
     */
    public void setStoredDataType(int storedDataType) {
        this.storedDataType = storedDataType;
    }

    public String getColFamilyName(){
        if (null == colFamilyName){
            typeItems = type.split("\\.");
            colFamilyName = "Idx" + catMap.get(typeItems[0]) + structureMap.get(typeItems[1]) + 
                    indexedDataTypeMap.get(typeItems[2]) + storedDataTypeMap.get(typeItems[3]);
        }
        return colFamilyName;
    }
    
    public boolean isCatIndexName(){
        return typeItems[0].equals("1");
    }
    
    public boolean isCatIndexValue(){
        return typeItems[0].equals("2");
    }
    
    public boolean isStructureRowKey(){
        return typeItems[1].equals("1");
    }

    public boolean isStructureCachedCol(){
        return typeItems[1].equals("2");
    }

    public boolean isStructureSortedCol(){
        return typeItems[1].equals("3");
    }

    /**
     * @return the cachedColumnName
     */
    public String getCachedColumnName() {
        return cachedColumnName;
    }

    /**
     * @param cachedColumnName the cachedColumnName to set
     */
    public void setCachedColumnName(String cachedColumnName) {
        this.cachedColumnName = cachedColumnName;
    }

    /**
     * @return the indexKeysCached
     */
    public int getIndexKeysCached() {
        return indexKeysCached;
    }

    /**
     * @param indexKeysCached the indexKeysCached to set
     */
    public void setIndexKeysCached(int indexKeysCached) {
        this.indexKeysCached = indexKeysCached;
    }

    /**
     * @return the indexRowsCached
     */
    public int getIndexRowsCached() {
        return indexRowsCached;
    }

    /**
     * @param indexRowsCached the indexRowsCached to set
     */
    public void setIndexRowsCached(int indexRowsCached) {
        this.indexRowsCached = indexRowsCached;
    }
    
}
