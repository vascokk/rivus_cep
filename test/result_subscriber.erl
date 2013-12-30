-module(result_subscriber).

-compile([debug_info, export_all]).

-include_lib("eunit/include/eunit.hrl").


start_link() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).    

init([]) ->
    gproc:reg({p, l, {self(), result_subscribers}}),
    {ok, ok}.

handle_call(stop, From, State) ->
    {stop, normal, ok, State};
handle_call(get_result, From, State) ->
    {reply, {ok, State}, State}.

handle_info(timeout,State) ->
    {stop,normal,State};

handle_info(_Info, State) ->
    ?debugMsg(io_lib:format("Info: ~p~n",[_Info])),
    {noreply, _Info, 50000}.

terminate(_Reason, _State) ->
    ?debugMsg(io_lib:format("Reason: ~p~n",[_Reason])),
    ok.

