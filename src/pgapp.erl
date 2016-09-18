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
         trx/2, trx/3,
         with_transaction/2,
         with_transaction/3]).

-include_lib("epgsql/include/epgsql.hrl").

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
-define(STATE_KEY(PoolName), {?STATE_VAR, PoolName}).
-define(DEFAULT_POOL, epgsql_pool).
-define(TIMEOUT, 5000).

equery(Sql, Args) ->
    equery(?DEFAULT_POOL, Sql, Args, []).

equery(PoolName, Sql, Args, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, ?TIMEOUT),
    case get(?STATE_KEY(PoolName)) of
        undefined ->
            no_transaction(PoolName, fun(W) ->
                handle_query(W, Sql, Args, Opts)
            end, Timeout);
        W ->
            handle_query(W, Sql, Args, Opts)
    end.

% short names
trx(Fun, Opts) ->
    with_transaction(Fun, Opts).

trx(PoolName, Fun, Opts) ->
    with_transaction(PoolName, Fun, Opts).

with_transaction(Fun, Opts) ->
    with_transaction(?DEFAULT_POOL, Fun, Opts).

with_transaction(PoolName, Fun, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, ?TIMEOUT),
    try no_transaction(PoolName, fun(W) ->
        put(?STATE_KEY(PoolName), W),
        epgsql:with_transaction(W, fun(_C) -> Fun() end)
    end, Timeout)
    after
        erase(?STATE_KEY(PoolName))
    end.

no_transaction(PoolName, Fun, Timeout) ->
    try poolboy:transaction(PoolName, Fun, Timeout)
    catch
        timeout:_Reason ->
            {error, timeout}
    end.

handle_query(Conn, Query, Args, Opts0) ->
    Timeout = proplists:get_value(timeout, Opts0, ?TIMEOUT),
    Opts = Opts0 ++ [
        {return_maps, application:get_env(?MODULE, return_maps, false)},
        {datetime_to_msec, application:get_env(?MODULE, datetime_to_msec, false)}
    ],
    Resp = pgapp_worker:equery(Conn, Query, Args, Timeout),
    case proplists:get_value(return_maps, Opts, false) of
        true -> result_to_map(Resp, Opts);
        false -> Resp
    end.

result_to_map({ok, _Count, Fields, Rows}, _Opts) ->
    %% select ... returning .. statement
    result_to_map({ok, Fields, Rows}, _Opts);

result_to_map({ok, Fields, Rows}, Opts) ->
    RowFunc = case proplists:get_value(datetime_to_msec, Opts, false) of
        true -> fun handle_column_datetime/2;
        false -> fun handle_column/2
    end,
    {ok, [maps:from_list(lists:zipwith(RowFunc, Fields, tuple_to_list(Row))) || Row <- Rows]};

result_to_map({ok, Count}, _Opts) ->
    {ok, Count};

result_to_map({error, #error{codename=Codename, message=Message}}, _Opts) ->
    {error, #{
        codename=>Codename,
        message=>Message
    }};

result_to_map({error, timeout}, _Opts) ->
    {error, timeout}.

handle_column_datetime(#column{name=Name, type=timestamp}, Value) ->
    handle_column(Name, fix_datetime(Value));
handle_column_datetime(#column{name=Name, type=timestamptz}, Value) ->
    handle_column(Name, fix_datetime(Value));
handle_column_datetime(#column{name=Name}, Value) ->
    handle_column(Name, Value).

handle_column(Name, Value) ->
    {binary_to_atom(Name, utf8), Value}.

fix_datetime({{_,_,_},{_,_,_}}=Date) -> msec(Date);
fix_datetime(Term) -> Term.

-define(UNIXTIME_BASE,62167219200).
msec({{_,_,_}=YMD,{H,M,S}}) ->
    Trunc = trunc(S),
    Sec = calendar:datetime_to_gregorian_seconds({YMD, {H,M,Trunc}}) - ?UNIXTIME_BASE,
    MSec = trunc((S-Trunc) * 1000),
    (Sec * 1000) + MSec.

