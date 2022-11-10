grammar PartitionFilter;

root
   : expr EOF
   ;

expr
   : '(' expr ')' # DoNothing
   | IDENTITY IN '(' values ')' # InOp
   | IDENTITY comparisonOperator value  # CompareOp
   | expr op=( AND|OR ) expr # LogicOp
   ;

values
   : (value ',')* value
   ;

comparisonOperator
    : '=' | '>' | '<' | '<=' | '>=' | '<>'
    ;

value
   : LITERAL
   ;

A : 'a' | 'A';
D : 'd' | 'D';
I : 'i' | 'I';
N : 'n' | 'N';
O : 'o' | 'O';
R : 'r' | 'R';

IN  : I N;
AND : A N D;
OR  : O R;

WHITESPACE : [ \t\r\n]+    -> skip;
IDENTITY   : [a-zA-Z_][a-zA-Z_0-9\u0080-\uFFFF]*;
LITERAL    : '\''([a-zA-Z_0-9\u0080-\uFFFF\-]+)'\''
           | '"'([a-zA-Z_0-9\u0080-\uFFFF\-]+)'"'
           | [0-9]+;
