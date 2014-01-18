-module(rivus_cep_slide_tests).
-include_lib("eunit/include/eunit.hrl").
-include("rivus_cep.hrl").

-define(SIZE, 30).
-define(DOUBLE_SIZE, 60).
-define(RUNTIME, 90).
-define(READINGS, 10).

slide_test_() ->
    {setup,
     fun () -> rivus_cep_slide_ets:start_link(),
	       rivus_cep_slide_server_sup:start_link()
     end,
     fun (_) -> 
	     ok
     end,
     [{"Create sliding window",
       fun create/0},
      {"test sliding window",
       {timeout, 30, fun slide/0}},
      {"resize sliding window (expand)",
       {timeout, 30, fun expand_window/0}},
      {"resize sliding window (shrink)",
       {timeout, 30, fun shrink_window/0}}

     ]}.

create() ->
    Window = rivus_cep_slide:new(?SIZE),    
    ?assert(is_pid(Window#slide.server)),
    ?assertEqual(?SIZE, Window#slide.size),
    ?assertEqual(0, ets:info(Window#slide.reservoir, size)).

slide() ->
    %% don't want a trim to happen
    %% unless we call trim
    %% so kill the trim server process
    Window = rivus_cep_slide:new(?SIZE),
    ok = rivus_cep_slide_server:stop(Window#slide.server),
    Moments = lists:seq(1, ?RUNTIME),
    %% pump in 90 seconds worth of readings
    Moment = lists:foldl(fun(_X, Tick) ->
                                 Tock = tick(Tick),
                                 [rivus_cep_slide:update(Window, N) ||
                                     N <- lists:duplicate(?READINGS, Tock)],
                                 Tock end,
                         0,
                         Moments),
    %% are all readings in the table?
    check_table(Window, Moments),
    %% get values only returns last ?WINDOW seconds
    ExpectedValues = lists:sort(lists:flatten([lists:duplicate(?READINGS, N) ||
                                                  N <- lists:seq(?RUNTIME - ?SIZE, ?RUNTIME)])),
    Values = lists:sort(rivus_cep_slide:get_values(Window)),
    ?assertEqual(ExpectedValues, Values),
    %% trim the table
    Trimmed = rivus_cep_slide:trim(Window),
    ?assertEqual((?RUNTIME - ?SIZE - 1) * ?READINGS, Trimmed),
    check_table(Window, lists:seq(?RUNTIME - ?SIZE, ?RUNTIME)),
    %% increment the clock past the window
    tick(Moment, ?SIZE * 2),
    %% get values should be empty
    ?assertEqual([], rivus_cep_slide:get_values(Window)),
    %% trim, and table should be empty
    Trimmed2 = rivus_cep_slide:trim(Window),
    ?assertEqual((?RUNTIME * ?READINGS) - ((?RUNTIME - ?SIZE - 1) * ?READINGS), Trimmed2),
    check_table(Window, []),
    ok.

expand_window() ->
    %% create a new histogram
    %% will leave the trim server running, as resize() needs it
    Window = rivus_cep_slide:new(?SIZE),
    Moments = lists:seq(1, ?RUNTIME ),
    %% pump in 90 seconds worth of readings
    Moment = lists:foldl(fun(_X, Tick) ->
                                 Tock = tick(Tick),
                                 [rivus_cep_slide:update(Window, N) ||
                                     N <- lists:duplicate(?READINGS, Tock)],
                                 Tock end,
                         0,
                         Moments),
    %% are all readings in the table?
    check_table(Window, Moments),
    
    %% get values only returns last ?WINDOW seconds
    ExpectedValues = lists:sort(lists:flatten([lists:duplicate(?READINGS, N) ||
                                                  N <- lists:seq(?RUNTIME - ?SIZE, ?RUNTIME)])),
    Values = lists:sort(rivus_cep_slide:get_values(Window)),
    ?assertEqual(ExpectedValues, Values),

    %%expand the sliding window
    NewWindow = rivus_cep_slide:resize(Window, ?DOUBLE_SIZE),

    %% get values only returns last ?WINDOW*2 seconds
    NewExpectedValues = lists:sort(lists:flatten([lists:duplicate(?READINGS, N) ||
                                                  N <- lists:seq(?RUNTIME - ?DOUBLE_SIZE, ?RUNTIME)])),
    NewValues = lists:sort(rivus_cep_slide:get_values(NewWindow)),
    ?assertEqual(NewExpectedValues, NewValues),
        
    %% trim the table
    Trimmed = rivus_cep_slide:trim(NewWindow),
    ?assertEqual((?RUNTIME - ?DOUBLE_SIZE - 1) * ?READINGS, Trimmed),
    check_table(NewWindow, lists:seq(?RUNTIME - ?DOUBLE_SIZE, ?RUNTIME)),
    %% increment the clock past the window
    tick(Moment, ?DOUBLE_SIZE*2),
    %% get values should be empty
    ?assertEqual([], rivus_cep_slide:get_values(NewWindow)),
    %% trim, and table should be empty
    Trimmed2 = rivus_cep_slide:trim(NewWindow),
    ?assertEqual((?RUNTIME * ?READINGS) - ((?RUNTIME - ?DOUBLE_SIZE - 1) * ?READINGS), Trimmed2),
    check_table(NewWindow, []),
    ok.
%%    ok = folsom_metrics:delete_metric(?HISTO2).


shrink_window() ->
    %% create a new histogram
    %% will leave the trim server running, as resize() needs it
    Window = rivus_cep_slide:new(?DOUBLE_SIZE),
    Moments = lists:seq(1, ?RUNTIME ),
    %% pump in 90 seconds worth of readings
    Moment = lists:foldl(fun(_X, Tick) ->
                                 Tock = tick(Tick),
                                 [rivus_cep_slide:update(Window, N) ||
                                     N <- lists:duplicate(?READINGS, Tock)],
                                 Tock end,
                         0,
                         Moments),
    %% are all readings in the table?
    check_table(Window, Moments),
    
    %% get values only returns last ?DOUBLE_WINDOW seconds
    ExpectedValues = lists:sort(lists:flatten([lists:duplicate(?READINGS, N) ||
                                                  N <- lists:seq(?RUNTIME - ?DOUBLE_SIZE, ?RUNTIME)])),
    Values = lists:sort(rivus_cep_slide:get_values(Window)),
    ?assertEqual(ExpectedValues, Values),

    %%shrink the sliding window
    NewWindow = rivus_cep_slide:resize(Window, ?SIZE),

    %% get values only returns last ?SIZE seconds
    NewExpectedValues = lists:sort(lists:flatten([lists:duplicate(?READINGS, N) ||
                                                  N <- lists:seq(?RUNTIME - ?SIZE, ?RUNTIME)])),
    NewValues = lists:sort(rivus_cep_slide:get_values(NewWindow)),
    ?assertEqual(NewExpectedValues, NewValues),
    
    
    %% trim the table
    Trimmed = rivus_cep_slide:trim(NewWindow),
    ?assertEqual((?RUNTIME - ?SIZE - 1) * ?READINGS, Trimmed),
    check_table(NewWindow, lists:seq(?RUNTIME - ?SIZE, ?RUNTIME)),
    %% increment the clock past the window
    tick(Moment, ?SIZE*2),
    %% get values should be empty
    ?assertEqual([], rivus_cep_slide:get_values(NewWindow)),
    %% trim, and table should be empty
    Trimmed2 = rivus_cep_slide:trim(NewWindow),
    ?assertEqual((?RUNTIME * ?READINGS) - ((?RUNTIME - ?SIZE - 1) * ?READINGS), Trimmed2),
    check_table(NewWindow, []),
    ok.

tick(Moment0, IncrBy) ->
    Moment = Moment0 + IncrBy,
    meck:expect(rivus_cep_utils, timestamp, fun() ->
                                                  Moment end),
    Moment.

tick(Moment) ->
    tick(Moment, 1).

check_table(Window, Moments) ->
    Tab = lists:sort(ets:tab2list(Window#slide.reservoir)),
    {Ks, Vs} = lists:unzip(Tab),
    ExpectedVs = lists:sort(lists:flatten([lists:duplicate(10, N) || N <- Moments])),
    StrippedKeys = lists:usort([X || {X, _} <- Ks]),
    ?assertEqual(Moments, StrippedKeys),
    ?assertEqual(ExpectedVs, lists:sort(Vs)).
