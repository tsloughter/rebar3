-module(rebar_packages).

-export([get/2
        ,get_all_names/1
        ,get_package_versions/4
        ,get_package_deps/4
        ,new_package_table/0
        ,load_and_verify_version/1        
        ,registry_dir/1
        ,package_dir/1
        ,registry_checksum/4
        ,find_highest_matching/7
        ,find_highest_matching/5
        ,find_highest_matching_/7
        ,verify_table/1
        ,format_error/1
        ,update_package/3
        ,resolve_version/6]).

-ifdef(TEST).
-export([cmp_/4, cmpl_/4, valid_vsn/1]).
-endif.

-export_type([package/0]).

-include("rebar.hrl").
-include_lib("providers/include/providers.hrl").

-type pkg_name() :: binary() | atom().
-type vsn() :: binary().
-type package() :: pkg_name() | {pkg_name(), vsn()}.

format_error({missing_package, Name, Vsn}) ->
    io_lib:format("Package not found in registry: ~ts-~ts.", [rebar_utils:to_binary(Name), 
                                                              rebar_utils:to_binary(Vsn)]);
format_error({missing_package, Pkg}) ->
    io_lib:format("Package not found in registry: ~p.", [Pkg]).

-spec get(hex_core:config(), binary()) -> {ok, map()} | {error, term()}.
get(Config, Name) ->    
    case hex_api_package:get(Config, Name) of
        {ok, {200, _Headers, PkgInfo}} ->
            {ok, PkgInfo};
        _ ->
            {error, blewup}
    end.

-spec get_all_names(rebar_state:t()) -> [binary()].
get_all_names(State) ->    
    verify_table(State),
    lists:usort(ets:select(?PACKAGE_TABLE, [{#package{key={'$1', '_', '_'},
                                                      _='_'}, 
                                             [], ['$1']}])).

-spec get_package_versions(unicode:unicode_binary(), unicode:unicode_binary(), ets:tid(), rebar_state:t())
                          -> [vsn()].
get_package_versions(Dep, Repo, Table, State) ->
    ?MODULE:verify_table(State),
    ets:select(Table, [{#package{key={Dep,'$1', Repo},
                                 _='_'}, 
                        [], ['$1']}]).

-spec get_package(unicode:unicode_binary(), unicode:unicode_binary(),
                  binary() | undefined, unicode:unicode_binary(), ets:tab(), rebar_state:t())
                 -> {ok, #package{}} | none.
get_package(Dep, Vsn, undefined, Repo, Table, State) ->
    get_package(Dep, Vsn, '_', Repo, Table, State);
get_package(Dep, Vsn, Hash, Repo, Table, State) ->
    ?MODULE:verify_table(State),
    case ets:match_object(Table, #package{key={Dep, Vsn, Repo},
                                          checksum=Hash,
                                          _='_'}) of
        [Package] ->
            {ok, Package};
        _ ->
            none
    end.

new_package_table() ->    
    ?PACKAGE_TABLE = ets:new(?PACKAGE_TABLE, [named_table, public, ordered_set, {keypos, 2}]),
    ets:insert(?PACKAGE_TABLE, {?PACKAGE_INDEX_VERSION, package_index_version}).

-spec get_package_deps(unicode:unicode_binary(), unicode:unicode_binary(), vsn(), rebar_state:t())
                      -> [map()].
get_package_deps(Name, Vsn, Repo, State) ->
    try_lookup(?PACKAGE_TABLE, {Name, Vsn, Repo}, #package.dependencies, State).

-spec registry_checksum(unicode:unicode_binary(), vsn(), unicode:unicode_binary(), rebar_state:t())
                       -> binary().
registry_checksum(Name, Vsn, Repo, State) ->
    try_lookup(?PACKAGE_TABLE, {Name, Vsn, Repo}, #package.checksum, State).

try_lookup(Table, Key={_, _, Repo}, Element, State) ->
    ?MODULE:verify_table(State),
    try
        ets:lookup_element(Table, Key, Element)       
    catch
       _:_ ->
            handle_missing_package(Key, Repo, State, fun(_) ->
                                                             ets:lookup_element(Table, Key, Element)
                                                     end)
    end.

load_and_verify_version(State) ->
    {ok, RegistryDir} = registry_dir(State),
    case ets:file2tab(filename:join(RegistryDir, ?INDEX_FILE)) of
        {ok, _} ->
            case ets:lookup_element(?PACKAGE_TABLE, package_index_version, 1) of
                ?PACKAGE_INDEX_VERSION ->
                    true;
                V ->
                    %% no reason to confuse the user since we just start fresh and they
                    %% shouldn't notice, so log as a debug message only
                    ?DEBUG("Package index version mismatch. Current version ~p, this rebar3 expecting ~p",
                           [V, ?PACKAGE_INDEX_VERSION]),
                    (catch ets:delete(?PACKAGE_TABLE)),
                    new_package_table()                    
            end;
        _ ->            
            new_package_table()
    end.

handle_missing_package(PkgKey, Repo, State, Fun) ->
    Name = 
        case PkgKey of
            {N, Vsn, _Repo} ->
                ?DEBUG("Package ~ts-~ts not found. Fetching registry updates for "
                       "package and trying again...", [N, Vsn]),
                N;
            _ ->
                ?DEBUG("Package ~p not found. Fetching registry updates for "
                       "package and trying again...", [PkgKey]),
                PkgKey
        end,

    update_package(Name, Repo, State),
    try 
        Fun(State) 
    catch
        _:_ ->
            %% Even after an update the package is still missing, time to error out
            throw(?PRV_ERROR({missing_package, PkgKey}))
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
find_highest_matching(Dep, Constraint, Repo, Table, State) ->
    find_highest_matching(undefined, undefined, Dep, Constraint, Repo, Table, State).

find_highest_matching(Pkg, PkgVsn, Dep, Constraint, Repo, Table, State) ->
    try find_highest_matching_(Pkg, PkgVsn, Dep, Constraint, Repo, Table, State) of
        none ->
            handle_missing_package(Dep, Repo, State,
                                   fun(State1) ->
                                       find_highest_matching_(Pkg, PkgVsn, Dep, Constraint, Repo, Table, State1)
                                   end);
        Result ->
            Result
    catch
        _:_ ->
            handle_missing_package(Dep, Repo, State,
                                   fun(State1) ->
                                       find_highest_matching_(Pkg, PkgVsn, Dep, Constraint, Repo, Table, State1)
                                   end)
    end.

find_highest_matching_(Pkg, PkgVsn, Dep, Constraint, #{name := Repo}, Table, State) ->
    try get_package_versions(Dep, Repo, Table, State) of
        [Vsn] ->
            handle_single_vsn(Pkg, PkgVsn, Dep, Vsn, Constraint);
        Vsns ->
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

verify_table(State) ->
    ets:info(?PACKAGE_TABLE, named_table) =:= true orelse load_and_verify_version(State).

parse_deps(Deps) ->
    [{maps:get(app, D, Name), {pkg, Name, Constraint, undefined}} 
     || D=#{package := Name,
            requirement := Constraint} <- Deps].

parse_checksum(<<Checksum:256/big-unsigned>>) ->
    list_to_binary(
      rebar_string:uppercase(
        lists:flatten(io_lib:format("~64.16.0b", [Checksum]))));
parse_checksum(Checksum) ->
    Checksum.

update_package(Name, RepoConfig=#{name := Repo}, State) ->
    ?MODULE:verify_table(State),
    case hex_repo:get_package(RepoConfig, Name) of
        {ok, {200, _Headers, #{releases := Releases}}} ->
            _ = insert_releases(Name, Releases, Repo, ?PACKAGE_TABLE),
            {ok, RegistryDir} = rebar_packages:registry_dir(State),
            PackageIndex = filename:join(RegistryDir, ?INDEX_FILE),
            ok = ets:tab2file(?PACKAGE_TABLE, PackageIndex);
        _ ->
            fail
    end.

insert_releases(Name, Releases, Repo, Table) ->
    [true = ets:insert(Table,
                       #package{key={Name, Version, Repo},
                                checksum=parse_checksum(Checksum),
                                dependencies=parse_deps(Dependencies)})
     || #{checksum := Checksum,
          version := Version,
          dependencies := Dependencies} <- Releases].

-spec resolve_version(unicode:unicode_binary(), unicode:unicode_binary() | undefined,
                      binary() | undefined,
                      map(), ets:tab(), rebar_state:t())
                     -> {error, {invalid_vsn, unicode:unicode_binary()}} |
                        none |
                        {ok, #package{}}.
resolve_version(Dep, undefined, Hash, ResourceState, HexRegistry, State) ->
    Fun = fun(Repo) ->
              case highest_matching(Dep, "0", Repo, HexRegistry, State) of
                  none ->
                      not_found;
                  {ok, Vsn} ->
                      get_package(Dep, Vsn, Hash, Repo, HexRegistry, State)
              end
          end,
    handle_missing_no_exception(Fun, Dep, ResourceState, State);
resolve_version(Dep, DepVsn, Hash, ResourceState, HexRegistry, State) ->
    case valid_vsn(DepVsn) of
        false ->
            {error, {invalid_vsn, DepVsn}};
        _ ->
            Fun = fun(Repo) ->
                      case resolve_version_(Dep, DepVsn, Hash, Repo, HexRegistry, State) of
                          none ->
                              not_found;
                          {ok, Vsn} ->
                              get_package(Dep, Vsn, Hash, Repo, HexRegistry, State)
                      end
                  end,
            handle_missing_no_exception(Fun, Dep, ResourceState, State)
    end.

handle_missing_no_exception(Fun, Dep, ResourceState, State) ->
    #{repos := RepoConfigs,
      base_config := BaseConfig} = ResourceState,
    ec_lists:search(fun(Config=#{name := R}) ->
                            case Fun(R) of
                                not_found ->
                                    ?MODULE:update_package(Dep, maps:merge(BaseConfig, Config), State),
                                    Fun(R);
                                Result ->
                                    Result
                            end
                    end, RepoConfigs).

resolve_version_(Dep, DepVsn, Hash, Repo, HexRegistry, State) ->
    case DepVsn of
        <<"~>", Vsn/binary>> ->
            highest_matching(Dep, rm_ws(Vsn), Repo, HexRegistry, State);
        <<">=", Vsn/binary>> ->
            cmp(Dep, rm_ws(Vsn), Repo, HexRegistry, State, fun ec_semver:gte/2);
        <<">", Vsn/binary>> ->
            cmp(Dep, rm_ws(Vsn), Repo, HexRegistry, State, fun ec_semver:gt/2);
        <<"<=", Vsn/binary>> ->
            cmpl(Dep, rm_ws(Vsn), Repo, HexRegistry, State, fun ec_semver:lte/2);
        <<"<", Vsn/binary>> ->
            cmpl(Dep, rm_ws(Vsn), Repo, HexRegistry, State, fun ec_semver:lt/2);
        <<"==", Vsn/binary>> ->
            {ok, Vsn};
        Vsn ->
            {ok, Vsn}
    end.

rm_ws(<<" ", R/binary>>) ->
    rm_ws(R);
rm_ws(R) ->
    R.

valid_vsn(Vsn) ->
    %% Regepx from https://github.com/sindresorhus/semver-regex/blob/master/index.js
    SemVerRegExp = "v?(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(\\.(0|[1-9][0-9]*))?"
        "(-[0-9a-z-]+(\\.[0-9a-z-]+)*)?(\\+[0-9a-z-]+(\\.[0-9a-z-]+)*)?",
    SupportedVersions = "^(>=?|<=?|~>|==)?\\s*" ++ SemVerRegExp ++ "$",
    re:run(Vsn, SupportedVersions, [unicode]) =/= nomatch.

highest_matching(Dep, Vsn, Repo, HexRegistry, State) ->
    find_highest_matching_(undefined, undefined, Dep, Vsn, #{name => Repo}, HexRegistry, State).

cmp(Dep, Vsn, Repo, HexRegistry, State, CmpFun) ->
    case get_package_versions(Dep, Repo, HexRegistry, State) of
        [] ->
            none;
        Vsns ->
            cmp_(undefined, Vsn, Vsns, CmpFun)
    end.

cmp_(undefined, MinVsn, [], _CmpFun) ->
    MinVsn;
cmp_(HighestDepVsn, _MinVsn, [], _CmpFun) ->
    HighestDepVsn;

cmp_(BestMatch, MinVsn, [Vsn | R], CmpFun) ->
    case CmpFun(Vsn, MinVsn) of
        true ->
            cmp_(Vsn, Vsn, R, CmpFun);
        false  ->
            cmp_(BestMatch, MinVsn, R, CmpFun)
    end.

%% We need to treat this differently since we want a version that is LOWER but
%% the higest possible one.
cmpl(Dep, Vsn, Repo, HexRegistry, State, CmpFun) ->
    case get_package_versions(Dep, Repo, HexRegistry, State) of
        [] ->
            none;
        Vsns ->
            cmpl_(undefined, Vsn, Vsns, CmpFun)
    end.

cmpl_(undefined, MaxVsn, [], _CmpFun) ->
    MaxVsn;
cmpl_(HighestDepVsn, _MaxVsn, [], _CmpFun) ->
    HighestDepVsn;

cmpl_(undefined, MaxVsn, [Vsn | R], CmpFun) ->
    case CmpFun(Vsn, MaxVsn) of
        true ->
            cmpl_(Vsn, MaxVsn, R, CmpFun);
        false  ->
            cmpl_(undefined, MaxVsn, R, CmpFun)
    end;

cmpl_(BestMatch, MaxVsn, [Vsn | R], CmpFun) ->
    case CmpFun(Vsn, MaxVsn) of
        true ->
            case ec_semver:gte(Vsn, BestMatch) of
                true ->
                    cmpl_(Vsn, MaxVsn, R, CmpFun);
                false ->
                    cmpl_(BestMatch, MaxVsn, R, CmpFun)
            end;
        false  ->
            cmpl_(BestMatch, MaxVsn, R, CmpFun)
    end.
