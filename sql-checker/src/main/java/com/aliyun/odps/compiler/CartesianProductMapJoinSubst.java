package com.aliyun.odps.compiler;

import com.aliyun.odps.compiler.log.LogItem;
import com.aliyun.odps.mma.sql.SimpleSubst;
import com.aliyun.odps.mma.sql.Subst;

public class CartesianProductMapJoinSubst extends SubstScanner {

  @Override
  public Subst visit(Join s, LogItem l) {
    super.visit(s, l);

    if (s.getJoinToken().pt.pos() != l.getPos()) {
      return null;
    }

    String alias = null;
    if (s.getJoinType() == JoinType.LEFT_OUTER && s.getRHS() instanceof TableFactor) {
      alias = ((TableFactor) s.getRHS()).getAlias().name();
    } else if (s.getJoinType() == JoinType.RIGHT_OUTER && s.getLHS() instanceof TableFactor) {
      alias = ((TableFactor) s.getLHS()).getAlias().name();
    }

    if (alias == null) {
      return null;
    }

    SyntaxTreeNode ast = s;
    while (ast != null && !(ast instanceof Select)) {
      ast = ast.getParent();
    }
    if (ast == null) {
      return null;
    }
    Select select = (Select) ast;
    if (select.getSelect().getMapJoinHint() != null) {
      MapJoinHint hint = select.getSelect().getMapJoinHint();
      hint.getSmallTables().add(new Identifier(alias));
      return new SimpleSubst(hint.parseTreeInfo().pos().getPos(), hint.parseTreeInfo().endPos().getPos(), hint.toString());
    }
    return new SimpleSubst(select.getSelect().pos().getPos() + "select ".length(),
                           select.getSelect().pos().getPos() + "select ".length(),
                           new MapJoinHint(Token.EOF, new SyntaxTreeNodeList<>(new Identifier(alias))).toString());
  }
}
