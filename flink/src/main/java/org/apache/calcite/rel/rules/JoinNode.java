package org.apache.calcite.rel.rules;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.List;

public class JoinNode {
  private final Integer id;
  private final JoinNode left;
  private final JoinNode right;

  public JoinNode(int id) {
    // leaf node
    this.id = id;
    this.left = null;
    this.right = null;
  }

  public JoinNode(JoinNode left, JoinNode right) {
    // middle node
    this.id = null;
    this.left = left;
    this.right = right;
  }

  public void visit(List<Integer> ids) {
    if (id != null) {
      ids.add(id);
    } else {
      left.visit(ids);
      right.visit(ids);
    }
  }

  /**
   * Build JoinVertexes.
   */
  public int buildVertexes(
          int multiJoinIdx,
          List<LoptMultiJoin.Edge> unusedEdges,
          List<UserJoinOrderRule.Vertex> vertexes) {
    if (id != null) {
      // LeafVertexes have been created before, which are in vertexes.
      return id;
    } else {
      int leftId = left.buildVertexes(multiJoinIdx, unusedEdges, vertexes);
      int rightId = right.buildVertexes(multiJoinIdx, unusedEdges, vertexes);
      Preconditions.checkArgument(leftId < vertexes.size());
      Preconditions.checkArgument(rightId < vertexes.size());
      UserJoinOrderRule.Vertex leftVertex = vertexes.get(leftId);
      UserJoinOrderRule.Vertex rightVertex = vertexes.get(rightId);
      int id = vertexes.size();

      final ImmutableBitSet newFactors =
              leftVertex.factors
                      .rebuild()
                      .addAll(rightVertex.factors)
                      .set(id)
                      .build();

      final List<RexNode> conditions = Lists.newArrayList();
      final Iterator<LoptMultiJoin.Edge> edgeIterator = unusedEdges.iterator();
      while (edgeIterator.hasNext()) {
        LoptMultiJoin.Edge edge = edgeIterator.next();
        if (newFactors.contains(edge.factors)) {
          conditions.add(edge.condition);
          edgeIterator.remove();
        }
      }
      if (conditions.isEmpty()) {
        throw new IllegalArgumentException(
                "There is no join condition between " + left.toString() +
                        " and " + right.toString() +
                        " for " + (multiJoinIdx + 1) + "-th MultiJoin.");
      }

      final UserJoinOrderRule.Vertex newVertex =
              new UserJoinOrderRule.JoinVertex(id, leftId, rightId, newFactors,
                      ImmutableList.copyOf(conditions));
      vertexes.add(newVertex);

      final ImmutableBitSet merged = ImmutableBitSet.of(leftId, rightId);
      for (int i = 0; i < unusedEdges.size(); i++) {
        final LoptMultiJoin.Edge edge = unusedEdges.get(i);
        if (edge.factors.intersects(merged)) {
          ImmutableBitSet newEdgeFactors =
                  edge.factors
                          .rebuild()
                          .removeAll(newFactors)
                          .set(id)
                          .build();
          assert newEdgeFactors.cardinality() == 2;
          final LoptMultiJoin.Edge newEdge =
                  new LoptMultiJoin.Edge(edge.condition, newEdgeFactors, edge.columns);
          unusedEdges.set(i, newEdge);
        }
      }

      return id;
    }
  }

  @Override
  public String toString() {
    if (id != null) {
      return id.toString();
    } else {
      return "[" + left.toString() + "," + right.toString() + "]";
    }
  }

}

