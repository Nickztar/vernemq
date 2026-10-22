%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

%% Publishes the replicated wills (vmq_lwt_db) of a cluster node that has died
%% (opt-in, `lwt_fire_on_node_down = on`).
%%
%% A node can't tell a dead peer from one it merely can't reach (a netsplit),
%% and firing in a netsplit would publish false wills for clients that are
%% still connected. So a peer that stops answering over Erlang distribution is
%% probed directly, once per round:
%%
%% - its epmd or its distribution port refuses the connection, or epmd has no
%%   such node: the host is up and VerneMQ is not, so the node is dead.
%% - its distribution port accepts a TCP connection: VerneMQ is running and only
%%   the distribution link is broken, so this is a netsplit.
%% - no answer at all (the host is gone, or the network between the two is
%%   cut): it counts as dead only while this node can still reach a witness
%%   (`lwt_witness`, host:port, by default the Kubernetes API service when
%%   running in a pod), i.e. while this node is not the isolated one.
%%
%% The wills are published `lwt_fire_delay` seconds after the peer stopped
%% answering, by default 45 s, the time an MQTT client with a 30 s keepalive
%% gets before the broker publishes its will. Clients that reconnected in the
%% meantime are skipped. By then the peer must have been found dead twice in a
%% row, or, when it does not answer at all, without interruption for
%% `lwt_unreachable_confirm` seconds, so a short outage of the network or an
%% overloaded host never fires. Any answer resets the peer.
%%
%% The first answering member by name fires, once per outage of a peer. A node
%% that restarts under the same name (and so looks alive again to its peers)
%% publishes the wills of its previous incarnation itself, `lwt_fire_delay`
%% seconds after it started.
-module(vmq_lwt_watch).
-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

-export([start_link/0, probe/1, probe/2, verdict/2, decide/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(INTERVAL, 1000).
-define(RPC_TIMEOUT, 1000).
-define(PROBE_TIMEOUT, 2000).
-define(EPMD_PORT, 4369).

-type verdict() :: dead | unreachable | netsplit | isolated.
-type peer() :: #{
    since := integer(),
    unreachable_since := integer() | undefined,
    dead := non_neg_integer(),
    verdict := verdict() | undefined,
    fired := boolean()
}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% peers: #{Peer => peer()}, only for peers that do not answer.
%% stale: whether the previous incarnation's wills are still to be published.
init([]) ->
    erlang:send_after(?INTERVAL, self(), check),
    {ok, #{started => erlang:monotonic_time(millisecond), stale => true, peers => #{}}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check, State0) ->
    Now = erlang:monotonic_time(millisecond),
    #{peers := Peers} = State1 = maybe_fire_stale(Now, State0),
    State =
        case
            vmq_config:get_env(lwt_fire_on_node_down, false) andalso
                vmq_config:get_env(replicate_lwt, false)
        of
            true -> State1#{peers := check(Now, Peers)};
            false -> State1#{peers := #{}}
        end,
    erlang:send_after(?INTERVAL, self(), check),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

maybe_fire_stale(Now, #{stale := true, started := Started} = State) ->
    case vmq_config:get_env(replicate_lwt, false) andalso Now - Started >= delay() of
        true ->
            _ = vmq_lwt_db:fire_stale(),
            State#{stale := false};
        false ->
            State
    end;
maybe_fire_stale(_Now, State) ->
    State.

delay() ->
    1000 * vmq_config:get_env(lwt_fire_delay, 45).

check(Now, State) ->
    case vmq_peer_service:members() of
        Members when is_list(Members) -> check(Members -- [node()], Now, State);
        _ -> State
    end.

check(Peers, Now, State) ->
    Answers = erpc:multicall(Peers, erlang, node, [], ?RPC_TIMEOUT),
    Answering = [N || {N, {ok, N}} <- lists:zip(Peers, Answers)],
    Silent = Peers -- Answering,
    Opts = #{
        delay => delay(),
        confirm => 1000 * vmq_config:get_env(lwt_unreachable_confirm, 30)
    },
    maps:from_list([
        {Peer, step(Peer, maps:get(Peer, State, undefined), Now, Opts, Answering)}
     || Peer <- Silent
    ]).

step(Peer, Peer0, Now, Opts, Answering) ->
    Probe = probe(Peer),
    Verdict = verdict(Probe, fun witness_reachable/0),
    case Peer0 of
        #{verdict := Verdict} -> ok;
        _ -> ?LOG_INFO("cluster node ~p does not answer: ~p (~p)", [Peer, Verdict, Probe])
    end,
    case decide(Peer0, Verdict, Now, Opts) of
        {fire, Peer1} ->
            ?LOG_WARNING("cluster node ~p is dead, publishing its wills", [Peer]),
            maybe_fire(Peer, Answering),
            Peer1;
        {wait, Peer1} ->
            Peer1
    end.

%% Pure decision for one round of a peer that does not answer. Peer0 is
%% undefined in the first round.
-spec decide(peer() | undefined, verdict(), integer(), #{delay := integer(), confirm := integer()}) ->
    {fire | wait, peer()}.
decide(undefined, Verdict, Now, Opts) ->
    decide(
        #{
            since => Now,
            unreachable_since => undefined,
            dead => 0,
            verdict => undefined,
            fired => false
        },
        Verdict,
        Now,
        Opts
    );
decide(Peer0, Verdict, Now, #{delay := Delay, confirm := Confirm}) ->
    #{since := Since, unreachable_since := US0, dead := Dead0} = Peer0,
    {Dead, US} =
        case Verdict of
            dead -> {Dead0 + 1, US0};
            unreachable when US0 == undefined -> {0, Now};
            unreachable -> {0, US0};
            _ -> {0, undefined}
        end,
    Peer = Peer0#{unreachable_since := US, dead := Dead, verdict := Verdict},
    IsDead = (Dead >= 2) orelse (US =/= undefined andalso Now - US >= Confirm),
    case maps:get(fired, Peer) of
        false when IsDead, Now - Since >= Delay -> {fire, Peer#{fired := true}};
        _ -> {wait, Peer}
    end.

%% refused / noport: the host answered and VerneMQ isn't there.
%% listening: VerneMQ is there, only distribution is broken.
%% nothing answered: unreachable, or isolated if we can't reach the witness
%% either (then we may be the one that is cut off).
-spec verdict(refused | noport | listening | unreachable, fun(() -> boolean())) -> verdict().
verdict(refused, _) ->
    dead;
verdict(noport, _) ->
    dead;
verdict(listening, _) ->
    netsplit;
verdict(unreachable, WitnessReachable) ->
    case WitnessReachable() of
        true -> unreachable;
        false -> isolated
    end.

probe(Peer) ->
    probe(Peer, ?EPMD_PORT).

probe(Peer, EpmdPort) ->
    [Name, Host] = string:split(atom_to_list(Peer), "@"),
    case epmd_port(Name, Host, EpmdPort) of
        {port, Port} ->
            case connect(Host, Port) of
                ok -> listening;
                Other -> Other
            end;
        Other ->
            Other
    end.

%% Asks the peer's epmd for its distribution port (PORT_PLEASE2). Not
%% erl_epmd:port_please/3: that also returns noport when epmd is merely slow
%% (it gives up after 10 s), which would make an overloaded peer look dead.
%% Here only epmd's explicit "not registered" answer is noport.
epmd_port(Name, Host, EpmdPort) ->
    case gen_tcp:connect(Host, EpmdPort, [binary, {active, false}], ?PROBE_TIMEOUT) of
        {ok, Socket} ->
            Req = list_to_binary(Name),
            Reply =
                case gen_tcp:send(Socket, <<(byte_size(Req) + 1):16, $z, Req/binary>>) of
                    ok -> gen_tcp:recv(Socket, 2, ?PROBE_TIMEOUT);
                    Error -> Error
                end,
            Result =
                case Reply of
                    {ok, <<$w, 0>>} ->
                        case gen_tcp:recv(Socket, 2, ?PROBE_TIMEOUT) of
                            {ok, <<Port:16>>} -> {port, Port};
                            _ -> unreachable
                        end;
                    {ok, <<$w, _>>} ->
                        noport;
                    _ ->
                        unreachable
                end,
            gen_tcp:close(Socket),
            Result;
        {error, econnrefused} ->
            refused;
        {error, _} ->
            unreachable
    end.

connect(Host, Port) ->
    case gen_tcp:connect(Host, Port, [], ?PROBE_TIMEOUT) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            ok;
        {error, econnrefused} ->
            refused;
        {error, _} ->
            unreachable
    end.

witness_reachable() ->
    case witness() of
        {Host, Port} -> connect(Host, Port) == ok;
        undefined -> false
    end.

witness() ->
    case vmq_config:get_env(lwt_witness, "") of
        "" ->
            case {os:getenv("KUBERNETES_SERVICE_HOST"), os:getenv("KUBERNETES_SERVICE_PORT")} of
                {Host, Port} when is_list(Host), is_list(Port) -> {Host, list_to_integer(Port)};
                _ -> undefined
            end;
        HostPort ->
            [Host, Port] = string:split(HostPort, ":", trailing),
            {Host, list_to_integer(Port)}
    end.

maybe_fire(Peer, Answering) ->
    case lists:min([node() | Answering]) of
        Me when Me == node() ->
            vmq_lwt_db:fire(Peer);
        Other ->
            ?LOG_INFO("wills of ~p are published by ~p", [Peer, Other])
    end.
