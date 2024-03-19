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

import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.IsNormalized
import org.neo4j.cypher.internal.ast.IsNotNormalized
import org.neo4j.cypher.internal.ast.IsNotTyped
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astBinaryFold
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astChildListSet
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astCtxReduce
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astOpt
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astPairs
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astSeq
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.child
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.ctxChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.lastChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.nodeChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.nodeChildType
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.pos
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.AllPropertiesSelector
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentUnordered
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.InvalidNotEquals
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.NFCNormalForm
import org.neo4j.cypher.internal.expressions.NFDNormalForm
import org.neo4j.cypher.internal.expressions.NFKCNormalForm
import org.neo4j.cypher.internal.expressions.NFKDNormalForm
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.NormalForm
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathFactor
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertySelector
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
import org.neo4j.cypher.internal.expressions.StarQuantifier
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.UnaryAdd
import org.neo4j.cypher.internal.expressions.UnarySubtract
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableSelector
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParserListener
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.MapType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.NothingType
import org.neo4j.cypher.internal.util.symbols.NullType
import org.neo4j.cypher.internal.util.symbols.PathType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.RelationshipType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IterableHasAsScala

trait ExpressionBuilder extends CypherParserListener {

  final override def exitQuantifier(ctx: CypherParser.QuantifierContext): Unit = {
    val firstToken = nodeChild(ctx, 0).getSymbol
    ctx.ast = firstToken.getType match {
      case CypherParser.LCURLY =>
        if (ctx.from != null || ctx.to != null || ctx.COMMA() != null) {
          IntervalQuantifier(optUnsignedDecimalInt(ctx.from), optUnsignedDecimalInt(ctx.to))(pos(ctx))
        } else {
          FixedQuantifier(unsignedDecimalInt(nodeChild(ctx, 1).getSymbol))(pos(firstToken))
        }
      case CypherParser.PLUS  => PlusQuantifier()(pos(firstToken))
      case CypherParser.TIMES => StarQuantifier()(pos(firstToken))
    }
  }

  private def optUnsignedDecimalInt(token: Token): Option[UnsignedDecimalIntegerLiteral] = {
    if (token != null) Some(unsignedDecimalInt(token)) else None
  }

  private def unsignedDecimalInt(token: Token): UnsignedDecimalIntegerLiteral = {
    UnsignedDecimalIntegerLiteral(token.getText)(pos(token))
  }

  final override def exitAnonymousPattern(ctx: CypherParser.AnonymousPatternContext): Unit = {
    ctx.ast = ctxChild(ctx, 0) match {
      case peCtx: CypherParser.PatternElementContext      => PathPatternPart(peCtx.ast())
      case spCtx: CypherParser.ShortestPathPatternContext => spCtx.ast
      case _                                              => throw new IllegalStateException(s"Unexpected context $ctx")
    }
  }

  final override def exitShortestPathPattern(ctx: CypherParser.ShortestPathPatternContext): Unit = {
    val first = nodeChild(ctx, 0).getSymbol
    ctx.ast =
      ShortestPathsPatternPart(ctx.patternElement().ast(), first.getType != CypherParser.ALL_SHORTEST_PATH)(pos(first))
  }

  final override def exitPatternElement(ctx: CypherParser.PatternElementContext): Unit = {
    val size = ctx.children.size()
    if (size == 1) {
      ctx.ast = ctxChild(ctx, 0).ast[PathFactor]()
    } else {
      val p = pos(ctx)
      val parts = new mutable.ArrayDeque[PathFactor](1)
      var relPattern: RelationshipPattern = null
      var i = 0
      while (i < size) {
        ctx.children.get(i) match {
          case nCtx: CypherParser.NodePatternContext =>
            val nodePattern = nCtx.ast[NodePattern]()
            if (relPattern != null) {
              val lhs = parts.removeLast().asInstanceOf[SimplePattern]
              parts.addOne(RelationshipChain(lhs, relPattern, nodePattern)(p))
              relPattern = null
            } else {
              parts.addOne(nodePattern)
            }
          case relCtx: CypherParser.RelationshipPatternContext =>
            relPattern = relCtx.ast[RelationshipPattern]()
          case qCtx: CypherParser.QuantifierContext =>
            val emptyNodePattern = NodePattern(None, None, None, None)(relPattern.position)
            val part =
              PathPatternPart(RelationshipChain(emptyNodePattern, relPattern, emptyNodePattern)(relPattern.position))
            parts.addOne(QuantifiedPath(part, qCtx.ast(), None)(relPattern.position))
            relPattern = null
          case parenCtx: CypherParser.ParenthesizedPathContext =>
            parts.addOne(parenCtx.ast[ParenthesizedPath]())
          case other => throw new IllegalStateException(s"Unexpected child $other")
        }
        i += 1
      }
      ctx.ast =
        if (parts.size == 1) parts.head
        else PathConcatenation(ArraySeq.unsafeWrapArray(parts.toArray))(pos(ctx))
    }
  }

  private def selectorCount(node: TerminalNode, p: InputPosition): UnsignedDecimalIntegerLiteral =
    if (node == null) UnsignedDecimalIntegerLiteral("1")(p)
    else UnsignedDecimalIntegerLiteral(node.getText)(pos(node))

  final override def exitSelector(ctx: CypherParser.SelectorContext): Unit = {
    val p = pos(ctx)
    ctx.ast = ctx match {
      case anyShortestCtx: CypherParser.AnyShortestPathContext =>
        PatternPart.AnyShortestPath(selectorCount(anyShortestCtx.UNSIGNED_DECIMAL_INTEGER(), p))(p)
      case allShortestCtx: CypherParser.AllShortestPathContext =>
        PatternPart.AllShortestPaths()(pos(allShortestCtx))
      case anyCtx: CypherParser.AnyPathContext =>
        PatternPart.AnyPath(selectorCount(anyCtx.UNSIGNED_DECIMAL_INTEGER(), p))(p)
      case shortestGrpCtx: CypherParser.ShortestGroupContext =>
        PatternPart.ShortestGroups(selectorCount(shortestGrpCtx.UNSIGNED_DECIMAL_INTEGER(), p))(p)
      case allPathCtx: CypherParser.AllPathContext =>
        PatternPart.AllPaths()(p)
      case _ => throw new IllegalStateException(s"Unexpected context $ctx")
    }
  }

  final override def exitParenthesizedPath(ctx: CypherParser.ParenthesizedPathContext): Unit = {
    val p = pos(ctx)
    val pattern = astChild[PatternPart](ctx, 1) match {
      case p: NonPrefixedPatternPart => p
      case p: PatternPartWithSelector =>
        val pathPatternKind = if (ctx.quantifier() == null) "parenthesized" else "quantified"
        // TODO Error handling
        throw new RuntimeException(
          s"Path selectors such as `${p.selector.prettified}` are not supported within $pathPatternKind path patterns."
        )
    }
    val quantifier = ctx.quantifier()
    ctx.ast = if (quantifier != null) QuantifiedPath(pattern, quantifier.ast(), astOpt(ctx.expression()))(p)
    else ParenthesizedPath(pattern, astOpt(ctx.expression()))(p)
  }

  final override def exitProperties(
    ctx: CypherParser.PropertiesContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
  }

  final override def exitLeftArrow(
    ctx: CypherParser.LeftArrowContext
  ): Unit = {}

  final override def exitArrowLine(
    ctx: CypherParser.ArrowLineContext
  ): Unit = {}

  final override def exitRightArrow(
    ctx: CypherParser.RightArrowContext
  ): Unit = {}

  final override def exitPathLength(
    ctx: CypherParser.PathLengthContext
  ): Unit = {
    // This is weird, we should refactor range to be more sensible and not use nested options
    ctx.ast = if (ctx.DOTDOT() != null) {
      val from = if (ctx.from != null) Some(UnsignedDecimalIntegerLiteral(ctx.from.getText)(pos(ctx.from))) else None
      val to = if (ctx.to != null) Some(UnsignedDecimalIntegerLiteral(ctx.to.getText)(pos(ctx.to))) else None
      Some(org.neo4j.cypher.internal.expressions.Range(from, to)(from.map(_.position).getOrElse(pos(ctx))))
    } else if (ctx.single != null) {
      val single = Some(UnsignedDecimalIntegerLiteral(ctx.single.getText)(pos(ctx.single)))
      Some(org.neo4j.cypher.internal.expressions.Range(single, single)(pos(ctx)))
    } else None
  }

  final override def exitExpression(ctx: CypherParser.ExpressionContext): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast
      case 3 => Or(astChild(ctx, 0), astChild(ctx, 2))(pos(nodeChild(ctx, 1)))
      case _ => Ors(astChildListSet(ctx))(pos(ctx))
    }
  }

  final override def exitExpression11(ctx: CypherParser.Expression11Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = astBinaryFold[Expression](ctx, (lhs, token, rhs) => Xor(lhs, rhs)(pos(token)))
  }

  final override def exitExpression10(ctx: CypherParser.Expression10Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = astBinaryFold[Expression](ctx, (lhs, token, rhs) => And(lhs, rhs)(pos(token)))
  }

  final override def exitExpression9(ctx: CypherParser.Expression9Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.expression8() == lastChild(ctx))
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast
      case 2 => Not(astChild(ctx, 1))(pos(ctx))
      case _ => ctx.NOT().asScala.foldRight(lastChild[AstRuleCtx](ctx).ast[Expression]()) { case (not, acc) =>
          Not(acc)(pos(not.getSymbol))
        }
    }
  }

  final override def exitExpression8(ctx: CypherParser.Expression8Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast
      case 3 => binaryPredicate(ctxChild(ctx, 0).ast(), child(ctx, 1), child(ctx, 2))
      case _ =>
        Ands(ctx.children.asScala.toSeq.sliding(3, 2).map {
          case Seq(lhs: AstRuleCtx, token: TerminalNode, rhs: AstRuleCtx) => binaryPredicate(lhs.ast(), token, rhs)
          case _ => throw new IllegalStateException(s"Unexpected parse results $ctx")
        })(pos(nodeChild(ctx, 1)))
    }
  }

  private def binaryPredicate(lhs: Expression, token: TerminalNode, rhs: AstRuleCtx): Expression = {
    token.getSymbol.getType match {
      case CypherParser.EQ          => Equals(lhs, rhs.ast())(pos(token.getSymbol))
      case CypherParser.INVALID_NEQ => InvalidNotEquals(lhs, rhs.ast())(pos(token.getSymbol))
      case CypherParser.NEQ         => NotEquals(lhs, rhs.ast())(pos(token.getSymbol))
      case CypherParser.LE          => LessThanOrEqual(lhs, rhs.ast())(pos(token.getSymbol))
      case CypherParser.GE          => GreaterThanOrEqual(lhs, rhs.ast())(pos(token.getSymbol))
      case CypherParser.LT          => LessThan(lhs, rhs.ast())(pos(token.getSymbol))
      case CypherParser.GT          => GreaterThan(lhs, rhs.ast())(pos(token.getSymbol))
    }
  }

  final override def exitExpression7(ctx: CypherParser.Expression7Context): Unit = {
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast
      case _ =>
        val lhs = ctxChild(ctx, 0).ast[Expression]()
        ctxChild(ctx, 1) match {
          case strCtx: CypherParser.StringAndListComparisonContext =>
            stringAndListComparisonExpression(lhs, strCtx)
          case nullCtx: CypherParser.NullComparisonContext =>
            nullComparisonExpression(lhs, nullCtx)
          case typeCtx: CypherParser.TypeComparisonContext =>
            typeComparisonExpression(lhs, typeCtx)
          case nfCtx: CypherParser.NormalFormComparisonContext =>
            normalFormComparisonExpression(lhs, nfCtx.normalForm(), nfCtx.NOT() != null, pos(nfCtx))
          case _ => throw new IllegalStateException(s"Unexpected parse result $ctx")
        }
    }
  }

  private def stringAndListComparisonExpression(lhs: Expression, ctx: AstRuleCtx): Expression = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      ctx.isInstanceOf[CypherParser.StringAndListComparisonContext] ||
        ctx.isInstanceOf[CypherParser.WhenStringOrListContext]
    )
    val token = child[TerminalNode](ctx, 0).getSymbol
    val rhs = lastChild[AstRuleCtx](ctx).ast[Expression]()
    token.getType match {
      case CypherParser.REGEQ    => RegexMatch(lhs, rhs)(pos(token))
      case CypherParser.STARTS   => StartsWith(lhs, rhs)(pos(token))
      case CypherParser.ENDS     => EndsWith(lhs, rhs)(pos(token))
      case CypherParser.CONTAINS => Contains(lhs, rhs)(pos(token))
      case CypherParser.IN       => In(lhs, rhs)(pos(token))
    }
  }

  private def nullComparisonExpression(lhs: Expression, ctx: AstRuleCtx): Expression = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      ctx.isInstanceOf[CypherParser.NullComparisonContext] ||
        ctx.isInstanceOf[CypherParser.WhenNullContext]
    )
    if (nodeChildType(ctx, 1) != CypherParser.NOT) IsNull(lhs)(pos(ctx))
    else IsNotNull(lhs)(pos(ctx))
  }

  private def typeComparisonExpression(lhs: Expression, ctx: AstRuleCtx): Expression = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      ctx.isInstanceOf[CypherParser.TypeComparisonContext] ||
        ctx.isInstanceOf[CypherParser.WhenTypeContext]
    )
    val cypherType = lastChild[AstRuleCtx](ctx).ast[CypherType]()
    val not = child[ParseTree](ctx, 1) match {
      case n: TerminalNode => n.getSymbol.getType == CypherParser.NOT
      case _               => false
    }
    if (not) IsNotTyped(lhs, cypherType)(pos(ctx))
    else IsTyped(lhs, cypherType)(pos(ctx))
  }

  private def normalFormComparisonExpression(
    lhs: Expression,
    nfCtx: CypherParser.NormalFormContext,
    not: Boolean,
    p: InputPosition
  ): Expression = {
    val nf = astOpt[NormalForm](nfCtx, NFCNormalForm)
    if (not) IsNotNormalized(lhs, nf)(p)
    else IsNormalized(lhs, nf)(p)
  }

  final override def exitComparisonExpression6(ctx: CypherParser.ComparisonExpression6Context): Unit = {}

  final override def exitNormalForm(ctx: CypherParser.NormalFormContext): Unit = {
    ctx.ast = child[TerminalNode](ctx, 0).getSymbol.getType match {
      case CypherParser.NFC  => NFCNormalForm
      case CypherParser.NFD  => NFDNormalForm
      case CypherParser.NFKC => NFKCNormalForm
      case CypherParser.NFKD => NFKDNormalForm
    }

  }

  final override def exitExpression6(ctx: CypherParser.Expression6Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = astBinaryFold(ctx, binaryAdditive)
  }

  private def binaryAdditive(lhs: Expression, token: TerminalNode, rhs: Expression): Expression = {
    token.getSymbol.getType match {
      case CypherParser.PLUS  => Add(lhs, rhs)(pos(token.getSymbol))
      case CypherParser.MINUS => Subtract(lhs, rhs)(pos(token.getSymbol))
    }
  }

  final override def exitExpression5(ctx: CypherParser.Expression5Context): Unit = {
    ctx.ast = astBinaryFold(ctx, binaryMultiplicative)
  }

  private def binaryMultiplicative(lhs: Expression, token: TerminalNode, rhs: Expression): Expression = {
    token.getSymbol.getType match {
      case CypherParser.TIMES   => Multiply(lhs, rhs)(pos(token.getSymbol))
      case CypherParser.DIVIDE  => Divide(lhs, rhs)(pos(token.getSymbol))
      case CypherParser.PERCENT => Modulo(lhs, rhs)(pos(token.getSymbol))
    }
  }

  final override def exitExpression4(ctx: CypherParser.Expression4Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = astBinaryFold[Expression](ctx, (lhs, token, rhs) => Pow(lhs, rhs)(pos(token.getSymbol)))
  }

  final override def exitExpression3(ctx: CypherParser.Expression3Context): Unit = {
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast()
      case _ =>
        if (ctx.PLUS() != null) UnaryAdd(lastChild[AstRuleCtx](ctx).ast())(pos(ctx))
        else UnarySubtract(lastChild[AstRuleCtx](ctx).ast())(pos(ctx))
    }
  }

  final override def exitExpression2(
    ctx: CypherParser.Expression2Context
  ): Unit = {
    ctx.ast = ctx.children.size() match {
      case 1 => ctxChild(ctx, 0).ast
      case _ => astCtxReduce(ctx, postFix)
    }
  }

  // TODO All postfix should probably have positions that work in the same manner
  private def postFix(lhs: Expression, rhs: CypherParser.PostFixContext): Expression = {
    val p = lhs.position
    rhs match {
      case propCtx: CypherParser.PropertyPostfixContext => Property(lhs, ctxChild(propCtx, 1).ast())(p)
      case indexCtx: CypherParser.IndexPostfixContext =>
        ContainerIndex(lhs, ctxChild(indexCtx, 1).ast())(pos(ctxChild(indexCtx, 1)))
      case labelCtx: CypherParser.LabelPostfixContext => LabelExpressionPredicate(lhs, ctxChild(labelCtx, 0).ast())(p)
      case rangeCtx: CypherParser.RangePostfixContext =>
        ListSlice(lhs, astOpt(rangeCtx.fromExp), astOpt(rangeCtx.toExp))(pos(rhs))
      case _ => throw new IllegalStateException(s"Unexpected rhs $rhs")
    }
  }

  final override def exitPostFix(ctx: CypherParser.PostFixContext): Unit = {}

  final override def exitProperty(ctx: CypherParser.PropertyContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast[PropertyKeyName]()
  }

  final override def exitPropertyExpression(ctx: CypherParser.PropertyExpressionContext): Unit = {
    var result = Property(ctxChild(ctx, 0).ast(), ctxChild(ctx, 2).ast())(pos(ctx))
    val size = ctx.children.size(); var i = 4
    while (i < size) {
      val key = ctxChild(ctx, i).ast[PropertyKeyName]()
      result = Property(result, key)(key.position)
      i += 2
    }
    ctx.ast = result
  }

  final override def exitExpression1(ctx: CypherParser.Expression1Context): Unit = {
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast()
      case _ => null
    }
  }

  final override def exitCaseExpression(
    ctx: CypherParser.CaseExpressionContext
  ): Unit = {
    ctx.ast = CaseExpression(
      expression = None,
      alternatives = astSeq(ctx.caseAlternative()),
      default = astOpt(ctx.expression())
    )(pos(ctx))
  }

  final override def exitCaseAlternative(ctx: CypherParser.CaseAlternativeContext): Unit = {
    ctx.ast = (ctxChild(ctx, 1).ast, ctxChild(ctx, 3).ast)
  }

  final override def exitExtendedCaseExpression(ctx: CypherParser.ExtendedCaseExpressionContext): Unit = {
    val caseExp = astChild[Expression](ctx, 1)
    ctx.ast = CaseExpression(
      expression = Some(caseExp),
      alternatives = extendedCaseAlts(caseExp, ctx.extendedCaseAlternative()),
      default = astOpt(ctx.elseExp)
    )(pos(ctx))
  }

  private def extendedCaseAlts(
    lhs: Expression,
    ctxs: java.util.List[CypherParser.ExtendedCaseAlternativeContext]
  ): ArraySeq[(Expression, Expression)] = {
    val size = ctxs.size()
    val resultBuffer = new ArrayBuffer[(Expression, Expression)](size)
    var i = 0
    while (i < size) {
      extendedCaseAlt(resultBuffer, lhs, ctxs.get(i))
      i += 1
    }
    ArraySeq.unsafeWrapArray(resultBuffer.toArray)
  }

  private def extendedCaseAlt(
    buffer: ArrayBuffer[(Expression, Expression)],
    lhs: Expression,
    ctx: CypherParser.ExtendedCaseAlternativeContext
  ): Unit = {
    val size = ctx.children.size()
    var i = 1
    val thenExp = lastChild[AstRuleCtx](ctx).ast[Expression]()
    while (i < size) {
      ctx.children.get(i) match {
        case whenCtx: CypherParser.ExtendedWhenContext =>
          val newWhen = whenCtx match {
            case _: CypherParser.WhenEqualsContext =>
              Equals(lhs, astChild(whenCtx, 0))(pos(nodeChild(ctx, i - 1)))
            case _: CypherParser.WhenComparatorContext =>
              binaryPredicate(lhs, nodeChild(whenCtx, 0), ctxChild(whenCtx, 1))
            case _: CypherParser.WhenStringOrListContext =>
              stringAndListComparisonExpression(lhs, whenCtx)
            case _: CypherParser.WhenNullContext =>
              nullComparisonExpression(lhs, whenCtx)
            case _: CypherParser.WhenTypeContext =>
              typeComparisonExpression(lhs, whenCtx)
            case formCtx: CypherParser.WhenFormContext =>
              normalFormComparisonExpression(lhs, formCtx.normalForm(), formCtx.NOT() != null, pos(formCtx))
            case _ => throw new IllegalStateException(s"Unexepected context $whenCtx")
          }
          buffer.addOne(newWhen -> thenExp)
        case _ =>
      }
      i += 1
    }
  }

  final override def exitExtendedCaseAlternative(ctx: CypherParser.ExtendedCaseAlternativeContext): Unit = {}

  final override def exitExtendedWhen(ctx: CypherParser.ExtendedWhenContext): Unit = {}

  final override def exitListComprehension(ctx: CypherParser.ListComprehensionContext): Unit = {
    ctx.ast = ListComprehension(
      variable = ctxChild(ctx, 1).ast(),
      expression = ctxChild(ctx, 3).ast(),
      innerPredicate = if (ctx.whereExp != null) Some(ctx.whereExp.ast()) else None,
      extractExpression = if (ctx.barExp != null) Some(ctx.barExp.ast()) else None
    )(pos(ctx))
  }

  final override def exitPatternComprehension(
    ctx: CypherParser.PatternComprehensionContext
  ): Unit = {
    val variable = if (ctx.variable() != null) Some(ctx.variable().ast[LogicalVariable]()) else None
    val pathPatternNonEmpty = ctx.pathPatternNonEmpty().ast[RelationshipsPattern]()
    val whereExp = if (ctx.whereExp != null) Some(ctx.whereExp.ast[PatternExpression]()) else None
    val barExp = ctx.barExp.ast[Expression]()
    ctx.ast = PatternComprehension(variable, pathPatternNonEmpty, whereExp, barExp)(pos(ctx), None, None)
  }

  final override def exitPathPatternNonEmpty(
    ctx: CypherParser.PathPatternNonEmptyContext
  ): Unit = {
    val first = ctxChild(ctx, 0).ast[PathFactor]()
    val size = ctx.children.size()
    var part = first
    var i = 1
    var relPattern: RelationshipPattern = null
    while (i < size) {
      ctxChild(ctx, i) match {
        case relCtx: CypherParser.RelationshipPatternContext => relPattern = relCtx.ast()
        case nodeCtx: CypherParser.NodePatternContext =>
          part = RelationshipChain(
            part.asInstanceOf[SimplePattern],
            relPattern,
            nodeCtx.ast()
          )(pos(nodeCtx))
        case other => throw new IllegalStateException(s"Unexpected child $other")
      }
      i += 1
    }
    ctx.ast = RelationshipsPattern(part.asInstanceOf[RelationshipChain])(pos(ctx))
  }

  final override def exitPatternExpression(
    ctx: CypherParser.PatternExpressionContext
  ): Unit = {
    ctx.ast = PatternExpression(ctxChild(ctx, 0).ast())(None, None)
  }

  final override def exitReduceExpression(
    ctx: CypherParser.ReduceExpressionContext
  ): Unit = {
    val accumulator = ctxChild(ctx, 2).ast[LogicalVariable]()
    val variable = ctxChild(ctx, 6).ast[LogicalVariable]()
    val expression = ctxChild(ctx, 10).ast[Expression]()
    val init = ctxChild(ctx, 4).ast[Expression]()
    val collection = ctxChild(ctx, 8).ast[Expression]()
    ctx.ast = ReduceExpression(ReduceScope(accumulator, variable, expression)(pos(ctx)), init, collection)(pos(ctx))
  }

  final override def exitListItemsPredicate(ctx: CypherParser.ListItemsPredicateContext): Unit = {
    val p = pos(ctx)
    val variable = ctx.variable().ast[Variable]()
    val inExp = ctx.inExp.ast[Expression]()
    val where = if (ctx.whereExp != null) Some(ctx.whereExp.ast[Expression]()) else None
    ctx.ast = nodeChild(ctx, 0).getSymbol.getType match {
      case CypherParser.ALL    => AllIterablePredicate(variable, inExp, where)(p)
      case CypherParser.ANY    => AnyIterablePredicate(variable, inExp, where)(p)
      case CypherParser.NONE   => NoneIterablePredicate(variable, inExp, where)(p)
      case CypherParser.SINGLE => SingleIterablePredicate(variable, inExp, where)(p)
    }
  }

  final override def exitShortestPathExpression(ctx: CypherParser.ShortestPathExpressionContext): Unit = {
    ctx.ast = ShortestPathExpression(astChild(ctx, 0))
  }

  final override def exitParenthesizedExpression(
    ctx: CypherParser.ParenthesizedExpressionContext
  ): Unit = { ctx.ast = ctxChild(ctx, 1).ast }

  final override def exitMapProjection(
    ctx: CypherParser.MapProjectionContext
  ): Unit = {
    ctx.ast = MapProjection(ctx.variable().ast[Variable](), astSeq(ctx.mapProjectionElement()))(pos(ctx.LCURLY()))
  }

  final override def exitMapProjectionElement(
    ctx: CypherParser.MapProjectionElementContext
  ): Unit = {
    val colon = ctx.COLON()
    val variable = ctx.variable()
    val propertyKeyName = ctx.propertyKeyName()
    ctx.ast =
      if (colon != null) {
        val expr = ctx.expression().ast[Expression]()
        LiteralEntry(propertyKeyName.ast(), expr)(expr.position)
      } else if (variable != null) {
        VariableSelector(variable.ast())(pos(ctx))
      } else if (propertyKeyName != null) {
        val prop = propertyKeyName.ast[PropertyKeyName]()
        PropertySelector(prop)(prop.position)
      } else {
        AllPropertiesSelector()(pos(ctx.TIMES().getSymbol))
      }
  }

  final override def exitCountStar(
    ctx: CypherParser.CountStarContext
  ): Unit = {
    ctx.ast = CountStar()(pos(ctx))
  }

  final override def exitExistsExpression(
    ctx: CypherParser.ExistsExpressionContext
  ): Unit = {
    ctx.ast = ExistsExpression(subqueryBuilder(
      ctx.regularQuery(),
      ctx.matchMode(),
      ctx.whereClause(),
      ctx.patternList()
    ))(pos(ctx), None, None)
  }

  final override def exitCountExpression(
    ctx: CypherParser.CountExpressionContext
  ): Unit = {
    ctx.ast = CountExpression(subqueryBuilder(
      ctx.regularQuery(),
      ctx.matchMode(),
      ctx.whereClause(),
      ctx.patternList()
    ))(pos(ctx), None, None)
  }

  private def subqueryBuilder(
    regQuery: CypherParser.RegularQueryContext,
    matchMode: CypherParser.MatchModeContext,
    whereClause: CypherParser.WhereClauseContext,
    patternList: CypherParser.PatternListContext
  ): Query = {
    if (regQuery != null) regQuery.ast[Query]()
    else {
      val patternParts = patternList.ast[ArraySeq[PatternPart]]().map {
        case p: PatternPartWithSelector => p
        case p: NonPrefixedPatternPart  => PatternPartWithSelector(PatternPart.AllPaths()(p.position), p)
      }
      val patternPos = patternParts.head.position
      val where = astOpt[Where](whereClause)
      val finalMatchMode = astOpt(matchMode, MatchMode.default(patternPos))
      SingleQuery(
        ArraySeq(
          Match(optional = false, finalMatchMode, Pattern.ForMatch(patternParts)(patternPos), Seq.empty, where)(
            patternPos
          )
        )
      )(patternPos)
    }

  }

  final override def exitCollectExpression(
    ctx: CypherParser.CollectExpressionContext
  ): Unit = {
    ctx.ast = CollectExpression(ctx.regularQuery().ast[Query]())(pos(ctx), None, None)
  }

  final override def exitPropertyKeyName(ctx: CypherParser.PropertyKeyNameContext): Unit = {
    ctx.ast = PropertyKeyName(ctxChild(ctx, 0).ast())(pos(ctx))
  }

  final override def exitParameter(ctx: CypherParser.ParameterContext): Unit = {
    val parameterType = ctx.paramType match {
      case "STRING" => CTString
      case "MAP"    => CTMap
      case _        => CTAny
    }
    val name: String = child[ParseTree](ctx, 1) match {
      case strCtx: CypherParser.SymbolicNameStringContext => strCtx.ast()
      case node: TerminalNode                             => node.getText
      case _                                              => throw new IllegalStateException(s"Unexpected ctx $ctx")
    }
    ctx.ast = ExplicitParameter(name, parameterType)(pos(ctx))
  }

  final override def exitFunctionInvocation(
    ctx: CypherParser.FunctionInvocationContext
  ): Unit = {
    val nameSpace = ctx.namespace()
    val symbolicNameString = ctx.symbolicNameString()
    val distinct = if (ctx.DISTINCT() != null) true else false
    val expressions = astSeq[Expression](ctx.expression())
    ctx.ast = FunctionInvocation(
      nameSpace.ast(),
      FunctionName(symbolicNameString.ast())(pos(symbolicNameString)),
      distinct,
      expressions,
      ArgumentUnordered,
      ctx.parent.isInstanceOf[CypherParser.GraphReferenceContext]
    )(pos(ctx))
  }

  final override def exitNamespace(
    ctx: CypherParser.NamespaceContext
  ): Unit = {
    ctx.ast = Namespace(astSeq[String](ctx.symbolicNameString()).toList)(pos(ctx))
  }

  final override def exitVariable(
    ctx: CypherParser.VariableContext
  ): Unit = {
    ctx.ast = Variable(name = ctx.symbolicNameString().ast())(pos(ctx))
  }

  final override def exitYieldItem(
    ctx: CypherParser.YieldItemContext
  ): Unit = {}

  final override def exitYieldClause(
    ctx: CypherParser.YieldClauseContext
  ): Unit = {}

  final override def exitShowIndexesAllowBrief(
    ctx: CypherParser.ShowIndexesAllowBriefContext
  ): Unit = {}

  final override def exitShowIndexesNoBrief(
    ctx: CypherParser.ShowIndexesNoBriefContext
  ): Unit = {}

  final override def exitShowConstraintsAllowBriefAndYield(
    ctx: CypherParser.ShowConstraintsAllowBriefAndYieldContext
  ): Unit = {}

  final override def exitShowConstraintsAllowBrief(
    ctx: CypherParser.ShowConstraintsAllowBriefContext
  ): Unit = {}

  final override def exitShowConstraintsAllowYield(
    ctx: CypherParser.ShowConstraintsAllowYieldContext
  ): Unit = {}

  final override def exitShowProcedures(
    ctx: CypherParser.ShowProceduresContext
  ): Unit = {}

  final override def exitShowFunctions(
    ctx: CypherParser.ShowFunctionsContext
  ): Unit = {}

  final override def exitShowTransactions(
    ctx: CypherParser.ShowTransactionsContext
  ): Unit = {}

  final override def exitTerminateTransactions(
    ctx: CypherParser.TerminateTransactionsContext
  ): Unit = {}

  final override def exitShowSettings(
    ctx: CypherParser.ShowSettingsContext
  ): Unit = {}

  final override def exitStringsOrExpression(
    ctx: CypherParser.StringsOrExpressionContext
  ): Unit = {}

  final override def exitCreateConstraint(
    ctx: CypherParser.CreateConstraintContext
  ): Unit = {}

  final override def exitType(ctx: CypherParser.TypeContext): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = ctx.children.size() match {
      case 1 => ctxChild(ctx, 0).ast
      case _ =>
        val types = ctx.typePart().asScala.map(_.ast[CypherType]()).toSet
        if (types.size == 1) types.head.simplify else ClosedDynamicUnionType(types)(pos(ctx)).simplify
    }
  }

  final override def exitTypePart(ctx: CypherParser.TypePartContext): Unit = {
    var cypherType = ctx.typeName().ast[CypherType]()
    if (ctx.typeNullability() != null) cypherType = cypherType.withIsNullable(false)
    ctx.typeListSuffix().forEach { list =>
      cypherType = ListType(cypherType, list.ast())(pos(ctx))
    }
    ctx.ast = cypherType
  }

  final override def exitTypeName(ctx: CypherParser.TypeNameContext): Unit = {
    val size = ctx.children.size
    val p = pos(ctx)
    val firstToken = nodeChild(ctx, 0).getSymbol.getType
    ctx.ast = size match {
      case 1 => firstToken match {
          case CypherParser.NOTHING                          => NothingType()(p)
          case CypherParser.NULL                             => NullType()(p)
          case CypherParser.BOOLEAN                          => BooleanType(true)(p)
          case CypherParser.STRING                           => StringType(true)(p)
          case CypherParser.INT | CypherParser.INTEGER       => IntegerType(true)(p)
          case CypherParser.FLOAT                            => FloatType(true)(p)
          case CypherParser.DATE                             => DateType(true)(p)
          case CypherParser.DURATION                         => DurationType(true)(p)
          case CypherParser.POINT                            => PointType(true)(p)
          case CypherParser.NODE | CypherParser.VERTEX       => NodeType(true)(p)
          case CypherParser.RELATIONSHIP | CypherParser.EDGE => RelationshipType(true)(p)
          case CypherParser.MAP                              => MapType(true)(p)
          case CypherParser.PATH                             => PathType(true)(p)
          case CypherParser.ANY                              => AnyType(true)(p)
          case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
        }
      case 2 => firstToken match {
          case CypherParser.SIGNED   => IntegerType(true)(p)
          case CypherParser.PROPERTY => PropertyValueType(true)(p)
          case CypherParser.LOCAL => nodeChild(ctx, 1).getSymbol.getType match {
              case CypherParser.TIME     => LocalTimeType(true)(p)
              case CypherParser.DATETIME => LocalDateTimeType(true)(p)
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case CypherParser.ZONED => nodeChild(ctx, 1).getSymbol.getType match {
              case CypherParser.TIME     => ZonedTimeType(true)(p)
              case CypherParser.DATETIME => ZonedDateTimeType(true)(p)
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case CypherParser.ANY => nodeChild(ctx, 1).getSymbol.getType match {
              case CypherParser.NODE | CypherParser.VERTEX       => NodeType(true)(p)
              case CypherParser.RELATIONSHIP | CypherParser.EDGE => RelationshipType(true)(p)
              case CypherParser.MAP                              => MapType(true)(p)
              case CypherParser.VALUE                            => AnyType(true)(p)
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
        }
      case 3 => firstToken match {
          case CypherParser.TIME => nodeChild(ctx, 1).getSymbol.getType match {
              case CypherParser.WITH    => ZonedTimeType(true)(p)
              case CypherParser.WITHOUT => LocalTimeType(true)(p)
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case CypherParser.TIMESTAMP => nodeChild(ctx, 1).getSymbol.getType match {
              case CypherParser.WITH    => ZonedDateTimeType(true)(p)
              case CypherParser.WITHOUT => LocalDateTimeType(true)(p)
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case CypherParser.ANY => nodeChild(ctx, 1).getSymbol.getType match {
              case CypherParser.PROPERTY                => PropertyValueType(true)(p)
              case CypherParser.VALUE | CypherParser.LT => ctx.`type`().ast
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
        }
      case _ => firstToken match {
          case CypherParser.LIST | CypherParser.ARRAY => ListType(ctx.`type`().ast(), true)(p)
          case CypherParser.ANY                       => ctx.`type`().ast[CypherType]()
          case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
        }
    }
  }

  final override def exitTypeNullability(ctx: CypherParser.TypeNullabilityContext): Unit = {}

  final override def exitTypeListSuffix(ctx: CypherParser.TypeListSuffixContext): Unit = {
    ctx.ast = ctx.typeNullability() == null
  }

  final override def exitMap(ctx: CypherParser.MapContext): Unit =
    ctx.ast = MapExpression(astPairs(ctx.propertyKeyName(), ctx.expression()))(pos(ctx))

  final override def exitSymbolicNameString(
    ctx: CypherParser.SymbolicNameStringContext
  ): Unit = {
    ctx.ast = child[AstRuleCtx](ctx, 0).ast
  }

  final override def exitEscapedSymbolicNameString(ctx: CypherParser.EscapedSymbolicNameStringContext): Unit = {
    val textWithoutEscapes =
      ctx.start.getInputStream.getText(new Interval(ctx.start.getStartIndex + 1, ctx.stop.getStopIndex - 1))
    ctx.ast = textWithoutEscapes.replace("``", "`")
  }

  final override def exitUnescapedSymbolicNameString(
    ctx: CypherParser.UnescapedSymbolicNameStringContext
  ): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount == 1)
    // ctx.getText is also correct
    // This implementation is a micro optimisation to not have to create a string builder.
    ctx.ast = ctx.children.get(0) match {
      case childCtx: CypherParser.UnescapedLabelSymbolicNameStringContext =>
        AssertMacros.checkOnlyWhenAssertionsAreEnabled(childCtx.getChildCount == 1)
        childCtx.children.get(0).getText
      case other => other.getText
    }
  }

  final override def exitSymbolicLabelNameString(
    ctx: CypherParser.SymbolicLabelNameStringContext
  ): Unit = {
    ctx.ast = child[AstRuleCtx](ctx, 0).ast
  }

  final override def exitUnescapedLabelSymbolicNameString(
    ctx: CypherParser.UnescapedLabelSymbolicNameStringContext
  ): Unit = {
    ctx.ast = ctx.getText
  }

  final override def exitNormalizeFunction(
    ctx: CypherParser.NormalizeFunctionContext
  ): Unit = {
    val expression = ctx.expression().ast[Expression]()
    val normalForm = astOpt[NormalForm](ctx.normalForm, NFCNormalForm).formName

    ctx.ast =
      FunctionInvocation(
        FunctionName("normalize")(pos(ctx)),
        distinct = false,
        IndexedSeq(
          expression,
          StringLiteral(normalForm)(pos(ctx), pos(ctx))
        )
      )(
        pos(ctx)
      )
  }

}
