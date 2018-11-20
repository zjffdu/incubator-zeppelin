package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Preconditions;

import java.util.List;

//import org.apache.calcite.rex.*;

public class UserJoinOrderRule extends RelOptRule {
  private List<JoinNode> joinOrders;
  private int multiJoinIdx = 0;

  public UserJoinOrderRule(List<JoinNode> joinOrders) {
    super(operand(MultiJoin.class, any()), RelFactories.LOGICAL_BUILDER, null);
    this.joinOrders = joinOrders;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final MultiJoin multiJoinRel = call.rel(0);
    final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();

    final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);
    final List<Vertex> vertexes = Lists.newArrayList();
    int x = 0;
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      final RelNode rel = multiJoin.getJoinFactor(i);
      vertexes.add(new LeafVertex(i, rel, x));
      x += rel.getRowType().getFieldCount();
    }
    assert x == multiJoin.getNumTotalFields();

    final List<LoptMultiJoin.Edge> unusedEdges = Lists.newArrayList();
    for (RexNode node : multiJoin.getJoinFilters()) {
      unusedEdges.add(multiJoin.createEdge(node));
    }
    Preconditions.checkArgument(multiJoinIdx < joinOrders.size());
    JoinNode joinNode = joinOrders.get(multiJoinIdx++);
    List<Integer> ids = Lists.newArrayList();
    joinNode.visit(ids);
    Preconditions.checkArgument(ids.size() == multiJoin.getNumJoinFactors());
    joinNode.buildVertexes(multiJoinIdx - 1, unusedEdges, vertexes);
    Preconditions.checkArgument(unusedEdges.isEmpty());

    List<Pair<RelNode, Mappings.TargetMapping>> relNodes = Lists.newArrayList();
    for (Vertex vertex : vertexes) {
      if (vertex instanceof LeafVertex) {
        LeafVertex leafVertex = (LeafVertex) vertex;
        final Mappings.TargetMapping mapping =
                Mappings.offsetSource(
                        Mappings.createIdentity(
                                leafVertex.rel.getRowType().getFieldCount()),
                        leafVertex.fieldOffset,
                        multiJoin.getNumTotalFields());
        relNodes.add(Pair.of(leafVertex.rel, mapping));
      } else {
        JoinVertex joinVertex = (JoinVertex) vertex;
        final Pair<RelNode, Mappings.TargetMapping> leftPair =
                relNodes.get(joinVertex.leftFactor);
        RelNode left = leftPair.left;
        final Mappings.TargetMapping leftMapping = leftPair.right;
        final Pair<RelNode, Mappings.TargetMapping> rightPair =
                relNodes.get(joinVertex.rightFactor);
        RelNode right = rightPair.left;
        final Mappings.TargetMapping rightMapping = rightPair.right;
        final Mappings.TargetMapping mapping =
                Mappings.merge(leftMapping,
                        Mappings.offsetTarget(rightMapping,
                                left.getRowType().getFieldCount()));
        final RexVisitor<RexNode> shuttle =
                new RexPermuteInputsShuttle(mapping, left, right);
        final RexNode condition =
                RexUtil.composeConjunction(rexBuilder, joinVertex.conditions, false);

        final RelNode join = relBuilder.push(left)
                .push(right)
                .join(JoinRelType.INNER, condition.accept(shuttle))
                .build();
        relNodes.add(Pair.of(join, mapping));
      }
    }

    final Pair<RelNode, Mappings.TargetMapping> top = Util.last(relNodes);
    relBuilder.push(top.left)
            .project(relBuilder.fields(top.right));
    call.transformTo(relBuilder.build());
  }

  /**
   * Participant in a join (relation or join).
   */
  abstract static class Vertex {
    final int id;
    protected final ImmutableBitSet factors;

    Vertex(int id, ImmutableBitSet factors) {
      this.id = id;
      this.factors = factors;
    }
  }

  /**
   * Relation participating in a join.
   */
  static class LeafVertex extends Vertex {
    private final RelNode rel;
    final int fieldOffset;

    LeafVertex(int id, RelNode rel, int fieldOffset) {
      super(id, ImmutableBitSet.of(id));
      this.rel = rel;
      this.fieldOffset = fieldOffset;
    }
  }

  /**
   * Participant in a join which is itself a join.
   */
  static class JoinVertex extends Vertex {
    private final int leftFactor;
    private final int rightFactor;
    /**
     * Zero or more join conditions. All are in terms of the original input
     * columns (not in terms of the outputs of left and right input factors).
     */
    final ImmutableList<RexNode> conditions;

    JoinVertex(int id, int leftFactor, int rightFactor, ImmutableBitSet factors,
               ImmutableList<RexNode> conditions) {
      super(id, factors);
      this.leftFactor = leftFactor;
      this.rightFactor = rightFactor;
      this.conditions = Preconditions.checkNotNull(conditions);
    }
  }
}

