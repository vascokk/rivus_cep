-record(res_eval_state, { stmt,
			  recno = 1,
			  aggrno = 1,
			  aggr_keys = orddict:new(),
			  res_keys = dict:new()
	 }).
