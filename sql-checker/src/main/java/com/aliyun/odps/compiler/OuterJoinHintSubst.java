package com.aliyun.odps.compiler;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.odps.compiler.log.LogItem;
import com.aliyun.odps.mma.sql.SimpleSubst;
import com.aliyun.odps.mma.sql.Subst;

public class OuterJoinHintSubst extends SubstScanner {

  @Override
  public Subst visit(Select s, LogItem l) {
    super.visit(s, l);

    if (!(s.getFrom().getOperand() instanceof Join)) {
      return null;
    }

    MapJoinHint hint = s.getSelect().getMapJoinHint();
    if (hint == null) {
      return null;
    }

    List<Identifier> smallTables = hint.getSmallTables();
    Join join = (Join) s.getFrom().getOperand();
    if ((join.getJoinType() == JoinType.LEFT_OUTER || join.getJoinType() == JoinType.FULL_OUTER)
        && join.getLHS().parseTreeInfo().pos() == l.getPos()) {
      smallTables = smallTables.stream().filter(
          a-> !a.equals(((TableFactor) join.getLHS()).getAlias())).collect(Collectors.toList());
    }

    if ((join.getJoinType() == JoinType.RIGHT_OUTER || join.getJoinType() == JoinType.FULL_OUTER)
        && join.getRHS().parseTreeInfo().pos() == l.getPos()) {
      smallTables = smallTables.stream().filter(
          a-> !a.equals(((TableFactor) join.getRHS()).getAlias())).collect(Collectors.toList());
    }

    if (smallTables.size() == hint.getSmallTables().size()) {
      return null;
    }

    if (smallTables.isEmpty()) {
      return new SimpleSubst(s.getSelect().pos().getPos() + "select".length() + 1,
                             s.getSelect().getColumns().get(0).parseTreeInfo().pos().getPos(), "");
    }

    hint.setSmallTables(new SyntaxTreeNodeList<>(smallTables));
    return new SimpleSubst(hint.parseTreeInfo().pos().getPos(), hint.parseTreeInfo().endPos().getPos(), hint.toString());
  }
}
