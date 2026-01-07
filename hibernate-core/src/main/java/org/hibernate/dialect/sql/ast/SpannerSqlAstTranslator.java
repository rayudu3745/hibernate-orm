/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.sql.ast;

import java.util.List;

import org.hibernate.Locking;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.IllegalQueryOperationException;
import org.hibernate.query.sqm.ComparisonOperator;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.spi.AbstractSqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.delete.DeleteStatement;
import org.hibernate.sql.ast.tree.expression.Any;
import org.hibernate.sql.ast.tree.expression.BinaryArithmeticExpression;
import org.hibernate.sql.ast.tree.expression.ColumnReference;
import org.hibernate.sql.ast.tree.expression.Every;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.Literal;
import org.hibernate.sql.ast.tree.expression.SqlTuple;
import org.hibernate.sql.ast.tree.expression.Summarization;
import org.hibernate.sql.ast.tree.from.DerivedTableReference;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.from.QueryPartTableReference;
import org.hibernate.sql.ast.tree.from.TableReference;
import org.hibernate.sql.ast.tree.predicate.InArrayPredicate;
import org.hibernate.sql.ast.tree.predicate.LikePredicate;
import org.hibernate.sql.ast.tree.insert.ConflictClause;
import org.hibernate.sql.ast.tree.insert.InsertSelectStatement;
import org.hibernate.sql.ast.tree.select.QueryPart;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.select.SelectClause;
import org.hibernate.sql.ast.tree.update.UpdateStatement;
import org.hibernate.sql.exec.spi.JdbcOperation;

/**
 * A SQL AST translator for Spanner.
 *
 * @author Christian Beikov
 */
public class SpannerSqlAstTranslator<T extends JdbcOperation> extends AbstractSqlAstTranslator<T> {

	// Spanner lacks the lateral keyword and instead has an unnest/array mechanism
	private boolean correlated;

	public SpannerSqlAstTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
		super( sessionFactory, statement );
	}

	@Override
	protected LockStrategy determineLockingStrategy(
			QuerySpec querySpec,
			Locking.FollowOn followOnLocking) {
		return LockStrategy.NONE;
	}

	@Override
	public void visitOffsetFetchClause(QueryPart queryPart) {
		renderLimitOffsetClause( queryPart );
	}

	@Override
	protected void renderComparison(Expression lhs, ComparisonOperator operator, Expression rhs) {
		if ( rhs instanceof Any && operator == ComparisonOperator.EQUAL ) {
			// Map "= ANY(subquery)" to "IN (subquery)"
			lhs.accept( this );
			appendSql( " in " );
			((Any) rhs).getSubquery().accept( this );
		}
		else if ( rhs instanceof Every && operator == ComparisonOperator.NOT_EQUAL ) {
			// ALL(subquery)" to "NOT IN (subquery)"
			lhs.accept( this );
			appendSql( " not in " );
			((Every) rhs).getSubquery().accept( this );
		}
		else {
			renderComparisonEmulateIntersect( lhs, operator, rhs );
		}
	}

	@Override
	protected void renderSelectTupleComparison(
			List<SqlSelection> lhsExpressions,
			SqlTuple tuple,
			ComparisonOperator operator) {
		emulateSelectTupleComparison( lhsExpressions, tuple.getExpressions(), operator, true );
	}

	@Override
	protected void renderPartitionItem(Expression expression) {
		if ( expression instanceof Literal ) {
			appendSql( "'0' || '0'" );
		}
		else if ( expression instanceof Summarization ) {
			// This could theoretically be emulated by rendering all grouping variations of the query and
			// connect them via union all but that's probably pretty inefficient and would have to happen
			// on the query spec level
			throw new UnsupportedOperationException( "Summarization is not supported by DBMS" );
		}
		else {
			expression.accept( this );
		}
	}

	@Override
	public void visitSelectClause(SelectClause selectClause) {
		getClauseStack().push( Clause.SELECT );

		try {
			appendSql( "select " );
			if ( correlated ) {
				appendSql( "as struct " );
			}
			if ( selectClause.isDistinct() ) {
				appendSql( "distinct " );
			}
			visitSqlSelections( selectClause );
		}
		finally {
			getClauseStack().pop();
		}
	}

	@Override
	protected void renderDerivedTableReference(DerivedTableReference tableReference) {
		final boolean correlated = tableReference.isLateral();
		final boolean oldCorrelated = this.correlated;
		if ( correlated ) {
			this.correlated = true;
			appendSql( "unnest(array" );
		}
		tableReference.accept( this );
		if ( correlated ) {
			this.correlated = oldCorrelated;
			appendSql( CLOSE_PARENTHESIS );
			// Spanner requires the alias to be outside the parentheses UNNEST(... ) alias
			super.renderTableReferenceIdentificationVariable( tableReference );
		}
	}

	@Override
	protected void visitDeleteStatementOnly(DeleteStatement statement) {
		// Spanner requires a WHERE in delete clause so we add "where true" if there is none
		if ( !hasWhere( statement.getRestriction() ) ) {
			renderDeleteClause( statement );
			appendSql( " where true" );
			visitReturningColumns( statement.getReturningColumns() );
		}
		else {
			super.visitDeleteStatementOnly( statement );
		}
	}

	@Override
	protected void visitUpdateStatementOnly(UpdateStatement statement) {
		// Spanner requires a WHERE in update clause so we add "where true" if there is none
		if ( !hasWhere( statement.getRestriction() ) ) {
			renderUpdateClause( statement );
			renderSetClause( statement.getAssignments() );
			appendSql( " where true" );
			visitReturningColumns( statement.getReturningColumns() );
		}
		else {
			super.visitUpdateStatementOnly( statement );
		}
	}

	@Override
	protected void renderTableReferenceIdentificationVariable(TableReference tableReference) {
		// Spanner requires `UNNEST(...) alias`. Standard rendering places the alias
		// inside the parentheses UNNEST(... alias). We suppress it here to manually
		// render it outside the UNNEST wrapper in `renderDerivedTableReference`.
		if ( correlated
			&& tableReference instanceof DerivedTableReference
			&& ((DerivedTableReference) tableReference).isLateral() ) {
			return;
		}
		super.renderTableReferenceIdentificationVariable( tableReference );
	}

	@Override
	protected void renderDmlTargetTableExpression(NamedTableReference tableReference) {
		super.renderDmlTargetTableExpression( tableReference );
		if ( getClauseStack().getCurrent() != Clause.INSERT ) {
			renderTableReferenceIdentificationVariable( tableReference );
		}
	}

	@Override
	protected void renderDerivedTableReferenceIdentificationVariable(DerivedTableReference tableReference) {
		renderTableReferenceIdentificationVariable( tableReference );
	}

	@Override
	public void visitQueryPartTableReference(QueryPartTableReference tableReference) {
		emulateQueryPartTableReferenceColumnAliasing( tableReference );
	}

	@Override
	public void visitInArrayPredicate(InArrayPredicate inArrayPredicate) {
		inArrayPredicate.getTestExpression().accept( this );
		appendSql( " in unnest(" );
		inArrayPredicate.getArrayParameter().accept( this );
		appendSql( ')' );
	}

	@Override
	public void visitLikePredicate(LikePredicate likePredicate) {
		if ( likePredicate.getEscapeCharacter() != null ) {
			throw new UnsupportedOperationException( "Escape character is not supported by Spanner" );
		}
		super.visitLikePredicate( likePredicate );
	}

	protected void visitConflictClause(ConflictClause conflictClause) {
		if ( conflictClause == null ) {
			return;
		}
		if ( conflictClause.getConstraintName() != null ) {
			throw new IllegalQueryOperationException(
					"Cloud Spanner does not support named constraints in conflict clauses." );
		}
		if ( !conflictClause.getConstraintColumnNames().isEmpty() ) {
			throw new IllegalQueryOperationException(
					"Cloud Spanner does not support specifying constraint columns in conflict clauses." );
		}
		if ( conflictClause.getPredicate() != null && !conflictClause.getPredicate().isEmpty() ) {
			throw new IllegalQueryOperationException(
					"Cloud Spanner does not support predicates (WHERE clause) in conflict clauses." );
		}
		// No-op for rendering: Spanner handles conflict via the INSERT OR UPDATE/IGNORE syntax
	}

	@Override
	protected void visitInsertStatementOnly(InsertSelectStatement statement) {
		final ConflictClause conflictClause = statement.getConflictClause();
		if ( conflictClause == null ) {
			super.visitInsertStatementOnly( statement );
			return;
		}
		visitConflictClause( conflictClause );
		getClauseStack().push( Clause.INSERT );
		if ( conflictClause.isDoUpdate() ) {
			appendSql( "insert or update into " );
		}
		else {
			appendSql( "insert or ignore into " );
		}
		renderDmlTargetTableExpression( statement.getTargetTable() );
		appendSql( OPEN_PARENTHESIS );
		boolean firstPass = true;
		final List<ColumnReference> targetColumnReferences = statement.getTargetColumns();
		for ( ColumnReference targetColumnReference : targetColumnReferences ) {
			if ( firstPass ) {
				firstPass = false;
			}
			else {
				appendSql( COMMA_SEPARATOR_CHAR );
			}
			appendSql( targetColumnReference.getColumnExpression() );
		}
		appendSql( ") " );
		getClauseStack().pop();
		visitInsertSource( statement );
		visitReturningColumns( statement.getReturningColumns() );
	}

	@Override
	public void visitBinaryArithmeticExpression(BinaryArithmeticExpression arithmeticExpression) {
		if ( isIntegerDivisionEmulationRequired( arithmeticExpression ) ) {
			// Spanner uses functional syntax: DIV(numerator, denominator)
			appendSql( "div(" );
			visitArithmeticOperand( arithmeticExpression.getLeftHandOperand() );
			appendSql( "," );
			visitArithmeticOperand( arithmeticExpression.getRightHandOperand() );
			appendSql( ")" );
		}
		else {
			super.visitBinaryArithmeticExpression( arithmeticExpression );
		}
	}

//	@Override
//	protected void visitReturningColumns(List<ColumnReference> returningColumns) {
//		if ( !returningColumns.isEmpty() ) {
//			appendSql( " then return " ); // Spanner specific syntax
//			boolean first = true;
//			for ( ColumnReference column : returningColumns ) {
//				if ( !first ) {
//					appendSql( ", " );
//				}
//				// Spanner requires simple column names in RETURNING, usually without table alias
//				column.accept( this );
//				first = false;
//			}
//		}
//	}

}
