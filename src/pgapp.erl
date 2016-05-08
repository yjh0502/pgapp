%%%-------------------------------------------------------------------
%%% @author David N. Welton <davidw@dedasys.com>
%%% @copyright (C) 2015, David N. Welton
%%% @doc
%%%
%%% @end
%%% Created : 20 Feb 2015 by David N. Welton <davidw@dedasys.com>
%%%-------------------------------------------------------------------
-module(pgapp).

%% API
-export([connect/2,
         equery/2, equery/4,
         with_transaction/2,
         with_transaction/3]).

%%%===================================================================
%%% API
%%%===================================================================
connect(PoolName, Settings) ->
    PoolSize    = proplists:get_value(size, Settings, 5),
    MaxOverflow = proplists:get_value(max_overflow, Settings, 5),
    pgapp_sup:add_pool(PoolName, [{name, {local, PoolName}},
                                  {worker_module, pgapp_worker},
                                  {size, PoolSize},
                                  {max_overflow, MaxOverflow}], Settings).

-define(STATE_VAR, '$pgapp_state').
-define(TIMEOUT, 5000).

equery(Sql, Args) ->
    equery(epgsql_pool, Sql, Args, ?TIMEOUT).

equery(PoolName, Sql, Args, Timeout) ->
    case get({?STATE_VAR, PoolName}) of
        undefined ->
            no_transaction(PoolName, fun(W) ->
                pgapp_worker:equery(W, Sql, Args, Timeout)
            end, Timeout);
        W ->
            pgapp_worker:equery(W, Sql, Args, Timeout)
    end.

with_transaction(Fun, Timeout) ->
    with_transaction(epgsql_pool, Fun, Timeout).

with_transaction(PoolName, Fun, Timeout) ->
    Resp = no_transaction(PoolName, fun(W) ->
        put({?STATE_VAR, PoolName}, W),
        epgsql:with_transaction(W, fun(_C) ->
            Fun()
        end)
    end, Timeout),
    erase({?STATE_VAR, PoolName}),
    Resp.

no_transaction(PoolName, Fun, Timeout) ->
    try poolboy:transaction(PoolName, Fun, Timeout)
    catch
        timeout:_Reason ->
            {error, timeout}
    end.

