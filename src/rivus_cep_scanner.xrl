%% ; -*- mode: Erlang;-*-

Definitions.
D	= [0-9]
H	= [0-9a-fA-F]
U	= [A-Z]
L	= [a-z]
A	= ({U}|{L}|{D}|_)
WS	= ([\000-\s]|%.*)

Rules.

{D}+		:	{token,{integer,TokenLine,list_to_integer(TokenChars)}}.

{L}{A}*		:	Atom = list_to_atom(TokenChars),
			{token,case reserved_word(Atom) of
				   true -> case Atom of
					       'end' -> {end_token,{'end',TokenLine}};
					       _ -> {Atom, TokenLine}
					   end;
				   false -> {atom,TokenLine,Atom}
			       end}.

({U}|_){A}*	:	{token,{var,TokenLine,list_to_atom(TokenChars)}}.

"(\\\^.|\\.|[^"])*" :
			%% Strip quotes.
			S = lists:sublist(TokenChars, 2, TokenLen - 2),
			{token,{string,TokenLine,S}}.
\'(\\.|\\\n|[^'\n])*\' :
			%% Strip quotes.
			S = lists:sublist(TokenChars, 2, TokenLen - 2),
			{token,{string,TokenLine,S}}.

\$(\\{O}{O}{O}|\\\^.|\\.|.) :
			{token,{char,TokenLine,cc_convert(TokenChars)}}.
\+		:	{token,{'+',TokenLine}}.
\-		:	{token,{'-',TokenLine}}.
\*		:	{token,{'*',TokenLine}}.
\/		:	{token,{'/',TokenLine}}.
\(		:	{token,{'(',TokenLine}}.
\)		:	{token,{')',TokenLine}}.
\=		:	{token,{'=',TokenLine}}.
\<		:	{token,{'<',TokenLine}}.
\>		:	{token,{'>',TokenLine}}.
>=		:	{token,{'>=',TokenLine}}.
=<		:	{token,{'<=',TokenLine}}.
<>		:	{token,{'<>',TokenLine}}.
->		:	{token,{'->',TokenLine}}.

[]()[}{|!?/;:,.*+#<>=-] :
			{token,{list_to_atom(TokenChars),TokenLine}}.
\;{WS}		:	{end_token,{semicolon,TokenLine}}.
{WS}+		:	skip_token.

Erlang code.

-export([reserved_word/1]).

%% reserved_word(Atom) -> Bool
%%   return 'true' if Atom is an Erlang reserved word, else 'false'.

reserved_word('define') -> true;
reserved_word('as') -> true;
reserved_word('select') -> true;
reserved_word('from') -> true;
reserved_word('where') -> true;
reserved_word('within') -> true;
reserved_word('seconds') -> true;
reserved_word('and') -> true;
reserved_word('or') -> true;
reserved_word('not') -> true;
reserved_word('if') -> true;
reserved_word('foreach') -> true;
reserved_word('index') -> true;
reserved_word('of') -> true;
reserved_word('end') -> true;
reserved_word('sum') -> true;
reserved_word('count') -> true;
reserved_word('avg') -> true;
reserved_word('min') -> true;
reserved_word('max') -> true;
reserved_word(_) -> false.

cc_convert([$$,$\\|Cs]) ->
    hd(string_escape(Cs));
cc_convert([$$,C]) -> C.

string_gen([$\\|Cs]) ->
    string_escape(Cs);
string_gen([C|Cs]) ->
    [C|string_gen(Cs)];
string_gen([]) -> [].

string_escape([O1,O2,O3|S]) when
  O1 >= $0, O1 =< $7, O2 >= $0, O2 =< $7, O3 >= $0, O3 =< $7 ->
    [(O1*8 + O2)*8 + O3 - 73*$0|string_gen(S)];
string_escape([$^,C|Cs]) ->
    [C band 31|string_gen(Cs)];
string_escape([C|Cs]) when C >= $\000, C =< $\s ->
    string_gen(Cs);
string_escape([C|Cs]) ->
    [escape_char(C)|string_gen(Cs)].

escape_char($n) -> $\n;				%\n = LF
escape_char($r) -> $\r;				%\r = CR
escape_char($t) -> $\t;				%\t = TAB
escape_char($v) -> $\v;				%\v = VT
escape_char($b) -> $\b;				%\b = BS
escape_char($f) -> $\f;				%\f = FF
escape_char($e) -> $\e;				%\e = ESC
escape_char($s) -> $\s;				%\s = SPC
escape_char($d) -> $\d;				%\d = DEL
escape_char(C) -> C.
