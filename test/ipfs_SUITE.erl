-module(ipfs_SUITE).

-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

-export([add/1]).
-export([ls/1]).
-export([cat/1]).

-define(DATA_1MiB, <<0:8388608>>).
-define(DATA_100MiB, <<0:838860800>>).
-define(DATA_1MiB_MD5, <<182,216,27,54,10,86,114,216,12,39,67,15,57,21,62,44>>).
-define(DATA_100MiB_MD5, <<47,40,43,132,231,230,8,213,133,36,73,237,148,11,252,81>>).
-define(DATA_1MiB_HASH, <<"QmVkbauSDEaMP4Tkq6Epm9uW75mWm136n81YH8fGtfwdHU">>).

all() -> [add, ls, cat].

init_per_suite(Config) ->
    start_ipfs_container(),
    {ok, _Started} = application:ensure_all_started(ipfs),
    Config.

end_per_suite(Config) ->
    application:stop(ipfs),
    stop_ipfs_container(),
    Config.

add(_Config) ->
    {ok, Pid} = ipfs:start_link(#{ip => "127.0.0.1"}),
    ok = file:write_file("/tmp/ipfs_test", ?DATA_1MiB),
    {ok, #{
        <<"Hash">> := ?DATA_1MiB_HASH,
        <<"Name">> := <<"ipfs_test">>,
        <<"Size">> := <<"1048832">>
    }} = ipfs:add(Pid, <<"/tmp/ipfs_test">>),
    file:delete("/tmp/ipfs_test"),
    ipfs:stop(Pid).

ls(_Config) ->
    {ok, Pid} = ipfs:start_link(#{ip => "127.0.0.1"}),
    {ok, #{<<"Objects">> := _Objects}} = ipfs:ls(Pid, ?DATA_1MiB_HASH),
    ipfs:stop(Pid).

cat(_Config) ->
    {ok, Pid} = ipfs:start_link(#{ip => "127.0.0.1"}),
    ok = file:write_file("/tmp/ipfs_test", <<"hello ipfs">>),
    {ok, #{<<"Hash">> := Hash}} = ipfs:add(Pid, <<"/tmp/ipfs_test">>),
    {ok, <<"hello ipfs">>} = ipfs:cat(Pid, Hash),
    file:delete("/tmp/ipfs_test"),
    ipfs:stop(Pid).

start_ipfs_container() ->
    sh(["docker", "pull", "ipfs/go-ipfs:latest"], [0], 120000),
    sh([
        "docker", "run", "--rm", "-d",
        "--name", "ipfs_test",
        "-p", "5001:5001",
        "ipfs/go-ipfs:latest"
    ]),
    wait_ipfs_container().

wait_ipfs_container() ->
    {0, Data} = sh(["docker", "logs", "ipfs_test"]),
    Log = binary:split(Data, <<"\n">>, [global, trim_all]),
    case lists:last(Log) of
        <<"Daemon is ready">> -> ok;
        _ ->
            timer:sleep(500),
            wait_ipfs_container()
    end.

stop_ipfs_container() ->
    sh(["docker", "container", "kill", "ipfs_test"]).

sh(CmdList) ->
    sh(CmdList, [0], 5000).

sh(CmdList, AllowedStatuses, Timeout) ->
    Cmd = string:join(CmdList, " "),
    Port = erlang:open_port({spawn, Cmd}, [{line, 256}, exit_status, stderr_to_stdout]),
    {Status, Output} = sh_data(Port, Timeout, []),
    sh_result(Status, Output, AllowedStatuses).

sh_data(Port, Timeout, Acc) ->
    receive
        {Port, {exit_status, Status}} ->
            {Status, erlang:iolist_to_binary(lists:reverse(Acc))};
        {Port, {data, {eol, Line}}} ->
            sh_data(Port, Timeout, ["\n", Line | Acc])
    after Timeout ->
            exit(Port, kill),
            erlang:error({timeout, erlang:iolist_to_binary(lists:reverse(Acc))})
    end.

sh_result(Status, Output, Allowed) ->
    case lists:member(Status, Allowed) of
        true ->
            {Status, Output};
        false ->
            erlang:error({sh, Status, Output})
    end.
