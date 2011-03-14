/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
