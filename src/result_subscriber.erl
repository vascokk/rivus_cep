-module(result_subscriber).

-compile([debug_info, export_all]).

-include_lib("eunit/include/eunit.hrl").


start_link() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).    

init([]) ->
    gproc:reg({p, l, {self(), result_subscribers}}),
    {ok, ok}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(get_result, _From, State) ->
    {reply, {ok, State}, State, 50000}.

handle_info(timeout, State) ->
    {stop,normal,State};

handle_info(_Info, _State) ->
    ?debugMsg(io_lib:format("Info: ~p~n",[_Info])),
    {noreply, _Info, infinity}.

terminate(_Reason, _State) ->
    ?debugMsg(io_lib:format("Reason: ~p~n",[_Reason])),
    ok.

