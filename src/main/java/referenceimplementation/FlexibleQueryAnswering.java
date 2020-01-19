package referenceimplementation;

import clusteringbasedfragmentation.ClusteringAffinityFunction;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import rewriting.RelaxationAttributeSelectionFinder;

import java.util.ArrayList;

/**
 * This class provides the functionality of generalizing a given SQL query wrt to a
 * clustering-based fragmentation.
 */
public class FlexibleQueryAnswering {

    /**
     * Generalize a given SQL query wrt to a clustering-based fragmentation.
     *
     * @param sql              SQL query
     * @param affinityFunction Clustering affinity function
     * @return Generalized query
     * @throws JSQLParserException Error when parsing and generalizing the given query
     */
    public static String generalize(String sql, ClusteringAffinityFunction affinityFunction)
            throws JSQLParserException {

        // Parse the query
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect body = (PlainSelect) select.getSelectBody();
        Expression where = body.getWhere();

        RelaxationAttributeSelectionFinder finder = new RelaxationAttributeSelectionFinder();
        if (!finder.findRelaxationAttributeSelections(where)) {
            System.out.println("Could not find any relaxation attribute selection condition in the given query body: " +
                    body);  // DEBUG
            return sql;
        }

        // Generalize the query
        ArrayList<EqualsTo> selections = finder.getRelaxationAttributeSelections();
        return RelaxationSelectionGeneralizer.generalizeSelections(body, affinityFunction);
    }

}
