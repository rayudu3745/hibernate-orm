/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.sql.ast;

import org.hibernate.Locking;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.IllegalQueryOperationException;
import org.hibernate.query.sqm.ComparisonOperator;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.spi.AbstractSqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.delete.DeleteStatement;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.Literal;
import org.hibernate.sql.ast.tree.expression.SqlTuple;
import org.hibernate.sql.ast.tree.expression.Summarization;
import org.hibernate.sql.ast.tree.from.DerivedTableReference;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.predicate.InArrayPredicate;
import org.hibernate.sql.ast.tree.predicate.LikePredicate;
import org.hibernate.sql.ast.tree.select.QueryPart;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.select.SelectClause;
import org.hibernate.sql.ast.tree.update.UpdateStatement;
import org.hibernate.sql.exec.spi.JdbcOperation;

import java.util.List;

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
		renderComparisonEmulateIntersect( lhs, operator, rhs );
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
	protected void renderDmlTargetTableExpression(NamedTableReference tableReference) {
		super.renderDmlTargetTableExpression( tableReference );
		if ( getClauseStack().getCurrent() != Clause.INSERT ) {
			renderTableReferenceIdentificationVariable( tableReference );
		}
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
		if ( likePredicate.isCaseSensitive() ) {
			likePredicate.getMatchExpression().accept( this );
			if ( likePredicate.isNegated() ) {
				appendSql( " not" );
			}
			appendSql( " like " );
			renderLikePredicate( likePredicate );
		}
		else {
			// Spanner does not support ILIKE, so we use the emulation
			renderCaseInsensitiveLikeEmulation(
					likePredicate.getMatchExpression(),
					likePredicate.getPattern(),
					likePredicate.getEscapeCharacter(),
					likePredicate.isNegated()
			);
		}
	}

	@Override
	protected void renderLikePredicate(LikePredicate likePredicate) {
		if ( likePredicate.getEscapeCharacter() == null ) {
			super.renderLikePredicate( likePredicate );
		}
		else {
			renderLikePattern( likePredicate.getPattern(), likePredicate.getEscapeCharacter() );
		}
	}

	@Override
	protected void renderCaseInsensitiveLikeEmulation(Expression lhs, Expression rhs, Expression escapeCharacter, boolean negated) {
		appendSql( getDialect().getLowercaseFunction() );
		appendSql( OPEN_PARENTHESIS );
		lhs.accept( this );
		appendSql( CLOSE_PARENTHESIS );

		if ( negated ) {
			appendSql( " not" );
		}
		appendSql( " like " );

		if ( escapeCharacter == null ) {
			appendSql( getDialect().getLowercaseFunction() );
			appendSql( OPEN_PARENTHESIS );
			rhs.accept( this );
			appendSql( CLOSE_PARENTHESIS );
		}
		else {
			// For case-insensitive with custom escape, we transform the pattern and then lower-case it.
			// We do NOT render the 'escape' clause as Spanner doesn't support it.
			appendSql( getDialect().getLowercaseFunction() );
			appendSql( OPEN_PARENTHESIS );
			renderLikePattern( rhs, escapeCharacter );
			appendSql( CLOSE_PARENTHESIS );
		}
	}

	private void renderLikePattern(Expression pattern, Expression escape) {
		// Resolve escape char from the expression
		Character escapeChar = null;
		Object escapeValue = getLiteralValue( escape );
		if ( escapeValue instanceof Character ) {
			escapeChar = (Character) escapeValue;
		}
		else if ( escapeValue instanceof String ) {
			String s = (String) escapeValue;
			if ( !s.isEmpty() ) {
				escapeChar = s.charAt( 0 );
			}
		}

		if ( escapeChar == null ) {
			throw new IllegalQueryOperationException(
					"Could not resolve LIKE escape character for Spanner translation" );
		}

		// If the escape char is already backslash, we can treat it as normal
		if ( escapeChar == '\\' ) {
			pattern.accept( this );
			return;
		}

		// Resolve the pattern string
		String patternText = null;
		Object patternValue = getLiteralValue( pattern );
		if ( patternValue instanceof String ) {
			patternText = (String) patternValue;
		}

		if ( patternText == null ) {
			throw new IllegalQueryOperationException(
					"Could not resolve LIKE pattern for Spanner translation with custom escape character" );
		}

		// Rewrite the pattern: replace custom escape with backslash
		String converted = convertLikePattern( patternText, escapeChar );

		// Render as string literal using the Dialect's specific escaping rules
		getDialect().appendLiteral( this, converted );
	}

	private String convertLikePattern(String pattern, char escape) {
		StringBuilder sb = new StringBuilder( pattern.length() );
		for ( int i = 0; i < pattern.length(); i++ ) {
			char c = pattern.charAt( i );
			if ( c == '\\' ) {
				// Existing backslashes must be double-escaped to survive Spanner's parser
				sb.append( "\\\\" );
			}
			else if ( c == escape ) {
				// If we find the custom escape char, look ahead
				if ( i + 1 < pattern.length() ) {
					char next = pattern.charAt( i + 1 );
					if ( next == '%' || next == '_' ) {
						// Replace 'escape' + wildcard with '\' + wildcard
						sb.append( '\\' ).append( next );
						i++;
					}
					else if ( next == escape ) {
						// Escape the escape char itself
						sb.append( escape );
						i++;
					}
					else {
						sb.append( c );
					}
				}
				else {
					sb.append( c );
				}
			}
			else {
				sb.append( c );
			}
		}
		return sb.toString();
	}

}
