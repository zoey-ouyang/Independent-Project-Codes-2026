package org.example.cquirrel.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RelationSchema implements Serializable {

    private String name;
    private List<String> primaryKeyColumns = new ArrayList<>();
    private List<ForeignKey> foreignKeys = new ArrayList<>();

    public RelationSchema() {
    }

    public RelationSchema(String name, List<String> primaryKeyColumns, List<ForeignKey> foreignKeys) {
        this.name = name;
        this.primaryKeyColumns = new ArrayList<>(primaryKeyColumns);
        this.foreignKeys = new ArrayList<>(foreignKeys);
    }

    public String getName() {
        return name;
    }

    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    @Override
    public String toString() {
        return "RelationSchema{"
                + "name='" + name + '\''
                + ", primaryKeyColumns=" + primaryKeyColumns
                + ", foreignKeys=" + foreignKeys
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RelationSchema)) {
            return false;
        }
        RelationSchema that = (RelationSchema) o;
        return Objects.equals(name, that.name)
                && Objects.equals(primaryKeyColumns, that.primaryKeyColumns)
                && Objects.equals(foreignKeys, that.foreignKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, primaryKeyColumns, foreignKeys);
    }
}
