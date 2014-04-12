%%------------------------------------------------------------------------------
%% Copyright (c) 2013 Vasil Kolarov
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%------------------------------------------------------------------------------

-record(res_eval_state, { stmt,
			  recno = 1,
			  aggrno = 1,
			  aggr_nodes = orddict:new(),
			  result = dict:new()		  
	 }).



-record(query_plan, {
	  join_keys = orddict:new(),
	  fsm
	 }).

-record(query_details, {
	  clauses,
	  producers,
	  subscribers,
	  options,
	  event_window,
	  fsm_window,
	  window_register,
	  event_window_pid,
	  fsm_window_pid
	 }).
%%[QueryClauses, Producers, Subscribers, _Options, EventWindow, FsmWindow, GlobalWinReg]

-record(query_state,{
	  query_name,
	  query_type,
	  producers,
	  subscribers,
	  events = [] ,
	  timeout = 60,
	  query_ast,
	  window,
	  fsm_window, 
	  win_register,
	  event_win_pid,
	  fsm_win_pid,
	  query_plan = #query_plan{},
	  stream_filters
	 }).

-record(query_ast,{
	  select,
	  from,
	  where,
	  within
	 }).

-record(fsm,{
	  fsm_state,
	  fsm_graph,
	  fsm_events
	 }).


