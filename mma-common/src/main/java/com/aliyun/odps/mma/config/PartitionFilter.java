package com.aliyun.odps.mma.config;


import com.aliyun.odps.mma.sql.PartitionFilterLexer;
import com.aliyun.odps.mma.sql.PartitionFilterParser;
import lombok.Getter;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public class PartitionFilter {
    @Getter
    private final String filterExpr;
    private final ParseTree exprTree;
    private final SimpleErrorListener errorListener;
    private List<String> error = new ArrayList<>();

    public PartitionFilter(String filterExpr) {
        this.filterExpr = filterExpr;
        errorListener = new SimpleErrorListener();

        CharStream charStream = CharStreams.fromString(filterExpr);
        PartitionFilterLexer lexer = new PtFilterLexer(charStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        TokenStream tokens = new CommonTokenStream(lexer);

        PartitionFilterParser parser = new PartitionFilterParser(tokens);
        parser.removeErrorListeners();
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        this.exprTree = parser.root();
    }

    public boolean filter(String partitionValue) {
        PartitionFilterVisitor visitor  = new PartitionFilterVisitor(partitionValue);
        Boolean ret = visitor.visit(exprTree);

        error = new ArrayList<>(visitor.getErrMap().values());

        return ret;
    }

    public String getSyntaxErr() {
        return this.errorListener.errMsg;
    }

    public Optional<String> getTypeError() {
        if (this.error.size() > 0) {
            return Optional.of(String.join(";", this.error));
        }

        return Optional.empty();
    }

    public static void main(String[] args) {
//        String sql = "(p1 >= '2022-03-04' Or (p2 = 10 or p3 > 20)) and p1 iN ('2022-03-05', '2022-03-06')";
//        PartitionFilter pf = new PartitionFilter(sql);
//        String pt = "p1=2022-03-05/p2=10/p3=11";
//        boolean isOk = pf.filter(pt);
//        System.out.printf("%s is %s, error found: %s\n", pt, isOk, pf.getSyntaxErr());
//
//        String sql1 = "p1 = 'rZqtQ' and p2 = 6930";
//        PartitionFilter pf1 = new PartitionFilter(sql1);
//        String pt1 = "p1=rZqtQ/p2=6930";
//        boolean isOk1 = pf1.filter(pt1);
//        System.out.printf("%s is %s, error found: %s\n", pt1, isOk1, pf1.getSyntaxErr());

        String[] exprList = new String[] {
                "day>='2021-06-24",
        };

        for (String expr: exprList) {
            System.out.println(expr);
            PartitionFilter pf = new PartitionFilter(expr);
            System.out.println(pf.getSyntaxErr());
        }
    }

    static class PtFilterLexer extends PartitionFilterLexer {
        public PtFilterLexer(CharStream input) {
            super(input);
        }
        @Override
        public void notifyListeners(LexerNoViableAltException e) {
            String text = this._input.getText(Interval.of(this._tokenStartCharIndex, this._input.index() - 1));
            String msg = "token recognition error at: '" + this.getErrorDisplay(text) + "'";
            ANTLRErrorListener listener = this.getErrorListenerDispatch();
            listener.syntaxError(this, (Object)null, this._tokenStartLine, this._tokenStartCharPositionInLine, msg, e);
        }
    }

    static class SimpleErrorListener extends BaseErrorListener {
        @Getter
        private String errMsg;

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
            if (Objects.isNull(errMsg)) {
                errMsg = msg;
            }
        }
    }
}
