-module(rebar_packages).

-export([get/2
        ,packages/1
        ,update_package/2
        ,close_packages/0
        ,load_and_verify_version/1
        ,deps/3
        ,registry_dir/1
        ,package_dir/1
        ,registry_checksum/2
        ,find_highest_matching/6
        ,find_highest_matching/4
        ,find_highest_matching_/6
        ,find_all/3
        ,verify_table/1
        ,format_error/1]).

-export_type([package/0]).

-include("rebar.hrl").
-include_lib("providers/include/providers.hrl").

-type pkg_name() :: binary() | atom().
-type vsn() :: binary().
-type package() :: pkg_name() | {pkg_name(), vsn()}.

-spec get(hex_core:config(), binary()) -> {ok, maps:map()} | {error, term()}.
get(Config, Name) ->    
    case hex_api_package:get(Config, Name) of
        {ok, {200, _Headers, PkgInfo}} ->
            {ok, PkgInfo};
        _ ->
            {error, blewup}
    end.

-spec packages(rebar_state:t()) -> ets:tid().
packages(State) ->
    catch ets:delete(?PACKAGE_TABLE),
    case load_and_verify_version(State) of
        true ->
            ok;
        false ->
            ?DEBUG("Error loading package index.", []),
            handle_bad_index(State)
    end.

handle_bad_index(State) ->
    ?ERROR("Bad packages index. Trying to fix by updating the registry.", []),
    %% {ok, State1} = rebar_prv_update:do(State),
    case load_and_verify_version(State) of
        true ->
            ok;
        false ->
            %% Still unable to load after an update, create an empty registry
            new_package_table()            
    end.

close_packages() ->
    catch ets:delete(?PACKAGE_TABLE).

load_and_verify_version(State) ->
    {ok, RegistryDir} = registry_dir(State),
    case ets:file2tab(filename:join(RegistryDir, ?INDEX_FILE)) of
        {ok, _} ->
            case ets:lookup_element(?PACKAGE_TABLE, package_index_version, 1) of
                ?PACKAGE_INDEX_VERSION ->
                    true;
                _ ->
                    (catch ets:delete(?PACKAGE_TABLE)),
                    new_package_table()                    
            end;
        _ ->            
            new_package_table()
    end.

new_package_table() ->    
    ets:new(?PACKAGE_TABLE, [named_table, public, {keypos, 2}]).

deps(Name, Vsn, State) ->
    try
        deps_(Name, Vsn, State) 
    catch
       _:_ ->
            handle_missing_package({Name, Vsn}, State, fun(State1) -> deps_(Name, Vsn, State1) end)        
    end.

deps_(Name, Vsn, State) ->
    ?MODULE:verify_table(State),
    ets:lookup_element(?PACKAGE_TABLE, {rebar_utils:to_binary(Name), 
                                        rebar_utils:to_binary(Vsn)}, #package.dependencies).

parse_deps(Deps) ->
    [{Name, Constraint} || #{package := Name,
                             requirement := Constraint} <- Deps].

parse_checksum(<<X:256/big-unsigned>>) ->
    list_to_binary(
      rebar_string:uppercase(
        lists:flatten(io_lib:format("~64.16.0b", [X])))).

update_package(Name, State) ->
    case hex_repo:get_package(hex_core:default_config(), Name) of
        {ok, {200, _Headers, #{releases := Releases}}} ->
            _Versions = [begin
                            true = ets:insert(?PACKAGE_TABLE, 
                                              #package{name_version={Name, Version},
                                                       checksum=parse_checksum(Checksum),
                                                       dependencies=parse_deps(Dependencies)}),
                            Version
                        end || #{checksum := Checksum,
                                 version := Version,
                                 dependencies := Dependencies} <- Releases],
            {ok, RegistryDir} = rebar_packages:registry_dir(State),
            PackageIndex = filename:join(RegistryDir, ?INDEX_FILE),
            ok = ets:tab2file(?PACKAGE_TABLE, PackageIndex);
        _ ->
            fail
    end.

handle_missing_package(Dep, State, Fun) ->
    Name = 
        case Dep of
            {N, Vsn} ->
                ?INFO("Package ~ts-~ts not found. Fetching registry updates for "
                      "package and trying again...", [N, Vsn]),
                N;
            _ ->
                ?INFO("Package ~p not found. Fetching registry updates for "
                      "package and trying again...", [Dep]),
                Dep
        end,

    %% {ok, State1} = rebar_prv_update:do(State),
    update_package(Name, State),
    try 
        Fun(State) 
    catch
        _:_ ->
            %% Even after an update the package is still missing, time to error out
            throw(?PRV_ERROR({missing_package, Dep}))
    end.

registry_dir(State) ->
    CacheDir = rebar_dir:global_cache_dir(rebar_state:opts(State)),
    case rebar_state:get(State, rebar_packages_cdn, ?DEFAULT_CDN) of
        ?DEFAULT_CDN ->
            RegistryDir = filename:join([CacheDir, "hex", "default"]),
            case filelib:ensure_dir(filename:join(RegistryDir, "placeholder")) of
                ok -> ok;
                {error, Posix} when Posix == eaccess; Posix == enoent ->
                    ?ABORT("Could not write to ~p. Please ensure the path is writeable.",
                           [RegistryDir])
            end,
            {ok, RegistryDir};
        CDN ->
            case rebar_utils:url_append_path(CDN, ?REMOTE_PACKAGE_DIR) of
                {ok, Parsed} ->
                    {ok, {_, _, Host, _, Path, _}} = http_uri:parse(Parsed),
                    CDNHostPath = lists:reverse(rebar_string:lexemes(Host, ".")),
                    CDNPath = tl(filename:split(Path)),
                    RegistryDir = filename:join([CacheDir, "hex"] ++ CDNHostPath ++ CDNPath),
                    ok = filelib:ensure_dir(filename:join(RegistryDir, "placeholder")),
                    {ok, RegistryDir};
                _ ->
                    {uri_parse_error, CDN}
            end
    end.

package_dir(State) ->
    case registry_dir(State) of
        {ok, RegistryDir} ->
            PackageDir = filename:join([RegistryDir, "packages"]),
            ok = filelib:ensure_dir(filename:join(PackageDir, "placeholder")),
            {ok, PackageDir};
        Error ->
            Error
    end.

registry_checksum({pkg, Name, Vsn, _Hash}, State) ->
    try
        ?MODULE:verify_table(State),
        ets:lookup_element(?PACKAGE_TABLE, {Name, Vsn}, #package.checksum) 
    catch
        _:_ ->
            throw(?PRV_ERROR({missing_package, rebar_utils:to_binary(Name), rebar_utils:to_binary(Vsn)}))
    end.

%% Hex supports use of ~> to specify the version required for a dependency.
%% Since rebar3 requires exact versions to choose from we find the highest
%% available version of the dep that passes the constraint.

%% `~>` will never include pre-release versions of its upper bound.
%% It can also be used to set an upper bound on only the major
%% version part. See the table below for `~>` requirements and
%% their corresponding translation.
%% `~>` | Translation
%% :------------- | :---------------------
%% `~> 2.0.0` | `>= 2.0.0 and < 2.1.0`
%% `~> 2.1.2` | `>= 2.1.2 and < 2.2.0`
%% `~> 2.1.3-dev` | `>= 2.1.3-dev and < 2.2.0`
%% `~> 2.0` | `>= 2.0.0 and < 3.0.0`
%% `~> 2.1` | `>= 2.1.0 and < 3.0.0`
find_highest_matching(Dep, Constraint, Table, State) ->
    find_highest_matching(undefined, undefined, Dep, Constraint, Table, State).

find_highest_matching(Pkg, PkgVsn, Dep, Constraint, Table, State) ->
    try find_highest_matching_(Pkg, PkgVsn, Dep, Constraint, Table, State) of
        none ->
            handle_missing_package(Dep, State,
                                   fun(State1) ->
                                       find_highest_matching_(Pkg, PkgVsn, Dep, Constraint, Table, State1)
                                   end);
        Result ->
            Result
    catch
        _:_ ->
            handle_missing_package(Dep, State,
                                   fun(State1) ->
                                       find_highest_matching_(Pkg, PkgVsn, Dep, Constraint, Table, State1)
                                   end)
    end.

find_highest_matching_(Pkg, PkgVsn, Dep, Constraint, Table, State) ->
    try find_all(Dep, Table, State) of
        {ok, [Vsn]} ->
            handle_single_vsn(Pkg, PkgVsn, Dep, Vsn, Constraint);
        {ok, Vsns} ->
            case handle_vsns(Constraint, Vsns) of
                none ->
                    none;
                FoundVsn ->
                    {ok, FoundVsn}
            end
    catch
        error:badarg ->
            none
    end.

find_all(Dep, Table, State) ->
    ?MODULE:verify_table(State),
    case ets:select(Table, [{#package{name_version={Dep,'$1'},
                                              _='_'}, 
                                     [], ['$1']}]) of
        [] ->
            none;
        Vsns ->
            {ok, Vsns}
    end.

handle_vsns(Constraint, Vsns) ->
    lists:foldl(fun(Version, Highest) ->
                        case ec_semver:pes(Version, Constraint) andalso
                            (Highest =:= none orelse ec_semver:gt(Version, Highest)) of
                            true ->
                                Version;
                            false ->
                                Highest
                        end
                end, none, Vsns).

handle_single_vsn(Pkg, PkgVsn, Dep, Vsn, Constraint) ->
    case ec_semver:pes(Vsn, Constraint) of
        true ->
            {ok, Vsn};
        false ->
            case {Pkg, PkgVsn} of
                {undefined, undefined} ->
                    ?DEBUG("Only existing version of ~ts is ~ts which does not match constraint ~~> ~ts. "
                          "Using anyway, but it is not guaranteed to work.", [Dep, Vsn, Constraint]);
                _ ->
                    ?DEBUG("[~ts:~ts] Only existing version of ~ts is ~ts which does not match constraint ~~> ~ts. "
                          "Using anyway, but it is not guaranteed to work.", [Pkg, PkgVsn, Dep, Vsn, Constraint])
            end,
            {ok, Vsn}
    end.

format_error({missing_package, Name, Vsn}) ->
    io_lib:format("Package not found in registry: ~ts-~ts.", [rebar_utils:to_binary(Name), rebar_utils:to_binary(Vsn)]);
format_error({missing_package, Dep}) ->
    io_lib:format("Package not found in registry: ~p.", [Dep]).

verify_table(State) ->
    ets:info(?PACKAGE_TABLE, named_table) =:= true orelse load_and_verify_version(State).
