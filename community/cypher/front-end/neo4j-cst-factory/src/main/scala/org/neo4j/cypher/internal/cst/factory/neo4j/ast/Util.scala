/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.cst.factory.neo4j.ast

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.util.InputPosition

object Util {
  @inline def cast[T](o: Any): T = o.asInstanceOf[T]

  @inline def child[T <: AstRuleCtx](ctx: AstRuleCtx, index: Int): T = ctx.getChild(index).asInstanceOf[T]
  @inline def nodeChild(ctx: AstRuleCtx, index: Int): TerminalNode = ctx.getChild(index).asInstanceOf[TerminalNode]

  @inline def lastChild[T <: ParseTree](ctx: AstRuleCtx): T =
    ctx.children.get(ctx.children.size() - 1).asInstanceOf[T]

  @inline def pos(token: Token): InputPosition =
    InputPosition(token.getStartIndex, token.getLine, token.getCharPositionInLine + 1)
  @inline def pos(ctx: ParserRuleContext): InputPosition = pos(ctx.start)
}