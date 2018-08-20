-module(rebar_prv_packages).

-behaviour(provider).

-export([init/1,
         do/1,
         format_error/1]).

-include("rebar.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("providers/include/providers.hrl").

-define(PROVIDER, pkgs).
-define(DEPS, []).

-spec init(rebar_state:t()) -> {ok, rebar_state:t()}.
init(State) ->
    State1 = rebar_state:add_provider(State, providers:create([{name, ?PROVIDER},
                                                               {module, ?MODULE},
                                                               {bare, true},
                                                               {deps, ?DEPS},
                                                               {example, "rebar3 pkgs"},
                                                               {short_desc, "List versions of a package."},
                                                               {desc, info("List versions of a package")},
                                                               {opts, []}])),
    {ok, State1}.

-spec do(rebar_state:t()) -> {ok, rebar_state:t()} | {error, string()}.
do(State) ->
    Config = hex_core:default_config(),    
    case rebar_state:command_args(State) of
        [Name] ->            
            print_packages(rebar_packages:get(Config, rebar_utils:to_binary(Name)));
        _ ->
            ok        
    end,
    {ok, State}.

-spec format_error(any()) -> iolist().
format_error(load_registry_fail) ->
    "Failed to load package regsitry. Try running 'rebar3 update' to fix".

print_packages({ok, #{<<"name">> := Name, <<"releases">> := Releases}}) ->
    Versions = [V || #{<<"version">> := V} <- Releases],
    VsnStr = join(Versions, <<", ">>),
    ?CONSOLE("~ts:~n    Versions: ~ts~n", [Name, VsnStr]);
print_packages(_) ->
    ok.

-spec join([binary()], binary()) -> binary().
join([Bin], _Sep) ->
    <<Bin/binary>>;
join([Bin | T], Sep) ->
    <<Bin/binary, Sep/binary, (join(T, Sep))/binary>>.


info(Description) ->
    io_lib:format("~ts.~n", [Description]).
