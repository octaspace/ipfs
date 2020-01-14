-module(ipfs).

-behaviour(gen_server).

-export([start_link/1]).
-export([stop/1]).

-export([ls/2]).
-export([ls/3]).
-export([add/2]).
-export([add/3]).
-export([cat/2]).
-export([cat/3]).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([handle_continue/2]).
-export([terminate/2]).

-record(state, {
    gun  :: undefined | pid(),
    opts :: map()
}).

start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

stop(Pid) ->
    gen_server:call(Pid, stop).

ls(Pid, Hash) ->
    ls(Pid, Hash, 5000).

ls(Pid, Hash, Timeout) ->
    Args = [
        {<<"arg">>, Hash}
    ],
    gen_server:call(Pid, {get, <<"/ls">>, Args}, Timeout).

add(Pid, File) ->
    add(Pid, File, 5000).

add(Pid, {data, Data}, Timeout) ->
    gen_server:call(Pid, {add_data, <<"/add">>, [], Data, Timeout}, Timeout);

add(Pid, File, Timeout) when is_binary(File) ->
    add(Pid, {file, File}, Timeout);

add(Pid, {file, File}, Timeout) ->
    BaseName = filename:basename(File),
    Args = [
        {<<"arg">>, BaseName}
    ],
    gen_server:call(Pid, {add_file, <<"/add">>, Args, File, BaseName, Timeout}, Timeout).

cat(Pid, Hash) ->
    cat(Pid, Hash, 5000).

cat(Pid, Hash, Timeout) ->
    Args = [
        {<<"arg">>, Hash}
    ],
    gen_server:call(Pid, {get, <<"/cat">>, Args}, Timeout).

init(Opts) ->
    {ok, #state{opts = Opts}, {continue, start_gun}}.

handle_call({get, URI, Args}, _From, State) ->
    StreamRef = gun:get(State#state.gun, format_uri(URI, Args)),
    Response = wait_response(State#state.gun, StreamRef),
    {reply, Response, State};

handle_call({add_file, URI, Args, File, BaseName, Timeout}, _From, State) ->
    case file:open(File, [read, binary, raw]) of
        {ok, FD} ->
            Boundary = cow_multipart:boundary(),
            StreamRef = gun:post(
                State#state.gun,
                format_uri(URI, Args),
                [{<<"content-type">>, <<"multipart/form-data; boundary=", Boundary/binary>>}]
            ),
            Data = iolist_to_binary([
                cow_multipart:part(Boundary, [
                    {<<"content-disposition">>, <<"form-data; name=\"filename\"; filename=\"", BaseName/binary, "\"">>},
                    {<<"content-type">>, <<"application/octet-stream">>}
                ])
            ]),
            gun:data(State#state.gun, StreamRef, nofin, Data),
            do_sendfile(State#state.gun, StreamRef, Boundary, FD),
            Response = wait_response(State#state.gun, StreamRef, Timeout),
            {reply, Response, State};
        Error ->
            {reply, Error, State}
    end;

handle_call({add_data, URI, Args, Data, Timeout}, _From, State) ->
    Boundary = cow_multipart:boundary(),
    StreamRef = gun:post(
        State#state.gun,
        format_uri(URI, Args),
        [{<<"content-type">>, <<"multipart/form-data; boundary=", Boundary/binary>>}]
    ),
    InitData = iolist_to_binary([
        cow_multipart:part(Boundary, [
            {<<"content-type">>, <<"application/octet-stream">>}
        ])
    ]),
    gun:data(State#state.gun, StreamRef, nofin, InitData),
    gun:data(State#state.gun, StreamRef, nofin, Data),
    gun:data(State#state.gun, StreamRef, fin, cow_multipart:close(Boundary)),
    Response = wait_response(State#state.gun, StreamRef, Timeout),
    {reply, Response, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Req, State) -> {noreply, State}.

handle_info({gun_up, _Pid, _Proto}, State) -> {noreply, State};

handle_info({gun_down, _Pid, _Proto, Reason, _KilledStreams, _UnprocessedStreams}, State) ->
    logger:error("connection down, reason: ~p", [Reason]),
    {noreply, State};

handle_info({'DOWN', _Ref, process, _Pid, Reason}, State) ->
    logger:error("connection terminated, reason: ~p", [Reason]),
    {noreply, State, {continue, start_gun}}.

handle_continue(start_gun, #state{opts = #{ip := IP} = Opts} = State) ->
    GunOpts = #{
        retry         => application:get_env(?MODULE, http_retry, 5),
        retry_timeout => application:get_env(?MODULE, http_retry_timeout, 5000),
        http_opts     => #{
            keepalive => infinity
        }
    },
    {ok, Gun} = gun:open(IP, maps:get(port, Opts, 5001), GunOpts),
    erlang:monitor(process, Gun),
    {noreply, State#state{gun = Gun}}.

terminate(_Reason, State) ->
    gun:close(State#state.gun).

format_uri(URI, QS) ->
    <<"/api/v0", URI/binary, "?", (cow_qs:qs(QS))/binary>>.

wait_response(Pid, StreamRef) ->
    wait_response(Pid, StreamRef, 5000).

wait_response(Pid, StreamRef, Timeout) ->
    case wait_response(Pid, StreamRef, undefined, undefined, <<>>, Timeout) of
        {ok, 200, Data} ->
            {ok, Data};
        {ok, _Status, Data} ->
            {error, Data};
        Error -> Error
    end.

wait_response(Pid, StreamRef, InitStatus, CT, Acc, Timeout) ->
    case gun:await(Pid, StreamRef, Timeout) of
        {response, nofin, Status, Headers} ->
            NewCT = proplists:get_value(<<"content-type">>, Headers, CT),
            wait_response(Pid, StreamRef, Status, NewCT, Acc, Timeout);
        {response, fin, Status, _Headers} ->
            {ok, Status, Acc};
        {data, nofin, Data} ->
            wait_response(Pid, StreamRef, InitStatus, CT, <<Acc/binary, Data/binary>>, Timeout);
        {data, fin, Data} when CT =:= <<"application/json">> ->
            {ok, InitStatus, jsx:decode(<<Acc/binary, Data/binary>>, [return_maps])};
        {data, fin, Data} ->
            {ok, InitStatus, <<Acc/binary, Data/binary>>};
        Error -> Error
    end.

do_sendfile(Pid, StreamRef, Boundary, FD) ->
    case file:read(FD, 1048576) of
        {ok, Data} ->
            gun:data(Pid, StreamRef, nofin, Data),
            do_sendfile(Pid, StreamRef, Boundary, FD);
        eof ->
            gun:data(Pid, StreamRef, fin, cow_multipart:close(Boundary)),
            file:close(FD)
    end.
