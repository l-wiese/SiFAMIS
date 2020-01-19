package rewriting;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class TableNamesAliasFinder extends TablesNamesFinder {

    /**
     * Returns the name of the table plus its alias if this is present.
     * @param table Table
     * @return "Tablename" or "Tablename Alias" (if present)
     */
    @Override
    protected String extractTableName(Table table) {
        if (table.getAlias() != null)
            return table.getName() + table.getAlias();
        return table.getName();
    }
}
