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
	 }).  %%will be extended later


-record(slide,{size,
	       reservoir = rivus_cep_slide_ets:new(rivus_slide, [duplicate_bag, {write_concurrency, true}, public]),
	       server}).
