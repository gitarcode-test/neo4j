/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsMatchResult
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

case class NodeProperty(offset: Int, token: Int) extends Expression with SlottedExpression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    state.query.nodeReadOps.getProperty(
      row.getLongAt(offset),
      token,
      state.cursors.nodeCursor,
      state.cursors.propertyCursor,
      throwOnDeleted = true
    )

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class NodePropertyLate(offset: Int, propKey: String) extends Expression with SlottedExpression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val maybeToken = state.query.getOptPropertyKeyId(propKey)
    if (maybeToken.isEmpty)
      Values.NO_VALUE
    else
      state.query.nodeReadOps.getProperty(
        row.getLongAt(offset),
        maybeToken.get,
        state.cursors.nodeCursor,
        state.cursors.propertyCursor,
        throwOnDeleted = true
      )
  }

  override def children: Seq[AstNode[_]] = Seq.empty

}

case class NodePropertyExists(offset: Int, token: Int) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    IsMatchResult(state.query.nodeReadOps.hasProperty(
      ctx.getLongAt(offset),
      token,
      state.cursors.nodeCursor,
      state.cursors.propertyCursor
    ))
  }

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class NodePropertyExistsLate(offset: Int, propKey: String) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    val maybeToken = state.query.getOptPropertyKeyId(propKey)
    val result =
      if (maybeToken.isEmpty)
        false
      else
        state.query.nodeReadOps.hasProperty(
          ctx.getLongAt(offset),
          maybeToken.get,
          state.cursors.nodeCursor,
          state.cursors.propertyCursor
        )
    IsMatchResult(result)
  }

  override def children: Seq[AstNode[_]] = Seq.empty
}
