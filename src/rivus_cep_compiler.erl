-module(rivus_cep_compiler).

-export([compile/2, scan_parse/4]).

compile (ScannerFile, ParserFile) ->
    {ok, ScanerModStr} = leex:file(ScannerFile),
    {ok, ParserModStr} = yecc:file(ParserFile).
    
scan_parse(Cont, Str, StartLoc, Acc) ->
	case erl_scan:tokens(Cont, Str, StartLoc) of 
		{done, {ok,Tokens, EndLoc}, LeftOverChars} -> 
			{ok,Form} = erl_parse:parse_form(Tokens),
			scan_parse([], LeftOverChars, EndLoc, [Form|Acc]);
		_ -> lists:reverse(Acc)
	end.
