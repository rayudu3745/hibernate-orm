/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.function.json;

import java.util.List;

import org.hibernate.QueryException;
import org.hibernate.metamodel.model.domain.ReturnableType;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.tree.SqlAstNode;
import org.hibernate.sql.ast.tree.expression.JsonNullBehavior;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Spanner-specific json_array function.
 * <p>
 * This override is needed because Spanner's native {@code json_array} function
 * does not support the {@code NULL ON NULL} or {@code ABSENT ON NULL} clauses
 * natively in the same way standard SQL does.
 * Therefore, this implementation strips out any un-emulatable or unsupported
 * standard SQL/JSON syntax and just delegates to the native {@code json_array(args...)}
 * without appending additional clauses.
 */
public class SpannerJsonArrayFunction extends JsonArrayFunction {

	public SpannerJsonArrayFunction(TypeConfiguration typeConfiguration) {
		super( typeConfiguration );
	}

	@Override
	public void render(
			SqlAppender sqlAppender,
			List<? extends SqlAstNode> sqlAstArguments,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> walker) {
		sqlAppender.appendSql( "json_array(" );
		if ( !sqlAstArguments.isEmpty() ) {
			final SqlAstNode lastArgument = sqlAstArguments.get( sqlAstArguments.size() - 1 );
			final int argumentsCount;
			if ( lastArgument instanceof JsonNullBehavior jsonNullBehavior ) {
				if ( jsonNullBehavior == JsonNullBehavior.ABSENT ) {
					throw new QueryException( "Spanner does not support ABSENT ON NULL behavior for json_array" );
				}
				argumentsCount = sqlAstArguments.size() - 1;
			}
			else {
				argumentsCount = sqlAstArguments.size();
			}
			for ( int i = 0; i < argumentsCount; i++ ) {
				if ( i > 0 ) {
					sqlAppender.appendSql( "," );
				}
				sqlAstArguments.get( i ).accept( walker );
			}
		}
		sqlAppender.appendSql( ")" );
	}
}
