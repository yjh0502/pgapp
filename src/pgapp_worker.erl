%% Worker for poolboy.  Initial code from
%% https://github.com/devinus/poolboy
%%
%% Copyright 2015 DedaSys LLC <davidw@dedasys.com>

-module(pgapp_worker).

-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([conn/2, equery/4]).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("epgsql/include/epgsql.hrl").

-define(INITIAL_DELAY, 500). % Half a second
-define(MAXIMUM_DELAY, 5 * 60 * 1000). % Five minutes
-define(TIMEOUT, 5 * 1000).

-record(state, {
    conn = undefined :: undefined | pid(),
    delay = ?INITIAL_DELAY :: pos_integer(),
    timer = undefined :: undefined | timer:tref(),
    start_args :: proplists:proplist(),
    statements = #{} :: map(),
    option = #{} ::map()
}).

conn(W, Timeout) ->
    gen_server:call(W, conn, Timeout).

equery(W, Sql, Args, Timeout) ->
    gen_server:call(W, {equery, Sql, Args}, Timeout).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),
    {ok, connect(#state{start_args = Args})}.

handle_call(conn, _From, #state{conn=Conn} = State) ->
    {reply, {ok, Conn}, State};

handle_call({equery, Sql, Args}, _From, State) ->
    exec_query(Sql, Args, State);

handle_call(Msg, From, #state{conn=Conn} = State) ->
    try gen_server:call(Conn, Msg, ?TIMEOUT) of
        Reply -> {reply, Reply, State}
    catch
        timeout:_ ->
            gen_server:reply(From, {error, timeout}),
            {noreply, connect(State)}
    end.

handle_cast(reconnect, State) ->
    {noreply, connect(State)}.

handle_info({'EXIT', From, Reason}, State) ->
    {NewDelay, Tref} =
        case State#state.timer of
            undefined ->
                %% We add a timer here only if there's not one that's
                %% already active.
                Delay = min(State#state.delay*2, ?MAXIMUM_DELAY),
                {ok, T} =
                    timer:apply_after(
                      State#state.delay,
                      gen_server, cast, [self(), reconnect]),
                {Delay, T};
            Timer ->
                {State#state.delay, Timer}
        end,

    error_logger:warning_msg(
      "~p EXIT from ~p: ~p - attempting to reconnect in ~p ms~n",
      [self(), From, Reason, NewDelay]),
    {noreply, State#state{conn=undefined, statements=#{}, delay = NewDelay, timer = Tref}}.

terminate(_Reason, #state{conn=Conn}) ->
    ok = epgsql:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

connect(State) ->
    Args = State#state.start_args,
    Hostname = proplists:get_value(host, Args),
    Database = proplists:get_value(database, Args),
    Username = proplists:get_value(username, Args),

    case epgsql:connect(Args) of
        {ok, Conn} ->
            error_logger:info_msg(
              "~p Connected to ~s at ~s with user ~s: ~p~n",
              [self(), Database, Hostname, Username, Conn]),
            timer:cancel(State#state.timer),
            State#state{conn=Conn, statements=#{}, delay=?INITIAL_DELAY, timer = undefined};
        Error ->
            NewDelay = min(State#state.delay*2, ?MAXIMUM_DELAY),
            error_logger:warning_msg(
              "~p Unable to connect to ~s at ~s with user ~s (~p) "
              "- attempting reconnect in ~p ms~n",
              [self(), Database, Hostname, Username, Error, NewDelay]),
            {ok, Tref} =
                timer:apply_after(
                  State#state.delay, gen_server, cast, [self(), reconnect]),
            State#state{conn=undefined, statements=#{}, delay = NewDelay, timer = Tref}
    end.

exec_query(Sql, Args, State = #state{statements=StmtMap,conn=Conn}) ->
    Name = stmt_name(Sql),
    case cache_prepare(Name, Sql, State) of
        {ok, Stmt = #statement{types = Types}, NextState} ->
            TypedArgs = lists:zip(Types, Args),
            Ref = epgsqla:prepared_query(Conn, Stmt, TypedArgs),
            receive
                {Conn, Ref, {error, #error{codename=invalid_sql_statement_name}}} ->
                    ok = epgsql:close(Conn, Stmt),
                    exec_query(Sql, Args, State#state{ statements=maps:remove(Name, StmtMap) });
                {Conn, Ref, {error, #error{codename=feature_not_supported}}} -> % cached plan must not change result type
                    ok = epgsql:close(Conn, Stmt),
                    exec_query(Sql, Args, State#state{ statements=maps:remove(Name, StmtMap) });
                {Conn, Ref, Resp} ->
                    {reply, Resp, NextState}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

cache_prepare(Name, Sql, State = #state{statements=StmtMap, conn=Conn}) ->
    case maps:get(Name, StmtMap, undefined) of
        undefined ->
            case epgsql:parse(Conn, Name, Sql, []) of
                {ok, Stmt2} ->
                    {ok, Stmt2, State#state{statements=StmtMap#{ Name => Stmt2 }}};
                {error, Reason} ->
                    {error, Reason}
            end;
        Stmt ->
            {ok, Stmt, State}
    end.

stmt_name(Sql) ->
    integer_to_binary(erlang:phash2(Sql), 36).
