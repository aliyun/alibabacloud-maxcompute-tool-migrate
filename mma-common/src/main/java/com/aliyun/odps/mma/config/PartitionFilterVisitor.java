package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.sql.PartitionFilterBaseVisitor;
import com.aliyun.odps.mma.sql.PartitionFilterParser;
import com.aliyun.odps.mma.util.StringUtils;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PartitionFilterVisitor extends PartitionFilterBaseVisitor<Boolean> {
    @Getter
    private final Map<String, String> errMap = new HashMap<>();
    private final Map<String, String> valueMap = new HashMap<>();

    public PartitionFilterVisitor(String partitionValue) {
        for (String kvStr: partitionValue.split("/")) {
            String[] kv = kvStr.split("=");
            valueMap.put(kv[0], kv[1]);
        }
    }

    @Override public Boolean visitRoot(PartitionFilterParser.RootContext ctx) {
        return visit(ctx.expr());
    }

    @Override public Boolean visitDoNothing(PartitionFilterParser.DoNothingContext ctx) {
        return visit(ctx.expr());
    }

    @Override
    public Boolean visitInOp(PartitionFilterParser.InOpContext ctx) {
        String partitionName = ctx.IDENTITY().getText();
        String realValue = valueMap.get(partitionName);

        if (Objects.isNull(realValue)) {
            return false;
        }

        return ctx.values().value().stream().anyMatch(v -> realValue.equals(trimValue(v.getText())));
    }

    @Override public Boolean visitCompareOp(PartitionFilterParser.CompareOpContext ctx) {
        String partitionName = ctx.IDENTITY().getText();
        String value = ctx.value().getText();
        String compare = ctx.comparisonOperator().getText();
        String realValue = valueMap.get(partitionName);

        if (Objects.isNull(realValue)) {
            return false;
        }

        if (isDigital(value)) {
            if (! isDigital(realValue)) {
                errMap.put(partitionName, String.format("value %s of %s is not int value, string must wrap using \" or ' ", value, partitionName));
                return false;
            }

            try {
                Long wantedLong = Long.parseLong(value);
                Long realLong  = Long.parseLong(realValue);

                switch (compare) {
                    case "<": return realLong < wantedLong;
                    case "<=": return realLong <= wantedLong;
                    case "=": return realLong.equals(wantedLong);
                    case ">": return realLong > wantedLong;
                    case ">=": return realLong >= wantedLong;
                    case "<>": return !realLong.equals(wantedLong);
                    default: return false;
                }

            } catch (NumberFormatException e) {
                errMap.put(partitionName, String.format("value %s of %s is not int value, string must wrap using \" or ' ", value, partitionName));
                return false;
            }
        }

        value = trimValue(value);

        switch (compare) {
            case "<": return realValue.compareTo(value) < 0;
            case "<=": return realValue.compareTo(value) <= 0;
            case "=": return realValue.compareTo(value) == 0;
            case ">": return realValue.compareTo(value) > 0;
            case ">=": return realValue.compareTo(value) >= 0;
            case "<>": return realValue.compareTo(value) != 0;
            default: return false;
        }
    }

    @Override public Boolean visitLogicOp(PartitionFilterParser.LogicOpContext ctx) {
        boolean v1 = visit(ctx.expr(0));
        boolean v2 = visit(ctx.expr(1));

        String logicOp = ctx.op.getText().toLowerCase();

        switch (logicOp) {
            case "and": return v1 && v2;
            case "or": return v1 || v2;
            default: return false;
        }
    }

    @Override public Boolean visitComparisonOperator(PartitionFilterParser.ComparisonOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override public Boolean visitValue(PartitionFilterParser.ValueContext ctx) {
        return visitChildren(ctx);
    }

    private boolean isDigital(String value) {
        return !(value.startsWith("'") || value.startsWith("\""));
    }

    private String trimValue(String value) {
        value = StringUtils.trim(value, "'");
        return  StringUtils.trim(value, "\"");
    }
}
