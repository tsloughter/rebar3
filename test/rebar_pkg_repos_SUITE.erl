%% Test suite for the handling hexpm repo configurations
-module(rebar_pkg_repos_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rebar.hrl").

all() ->
    [default_repo, repo_merging].

default_repo(_Config) ->
    Repo1 = #{name => <<"hexpm">>,
              api_key => <<"asdf">>},

    State = rebar_state:new(),
    State1 = rebar_state:set(State, hex, [{repos, [Repo1]}]),

    MergedRepos = rebar_pkg_resource:repos(State1),

    ?assertMatch([#{name := <<"hexpm">>,
                    api_key := <<"asdf">>,
                    api_url := <<"https://hex.pm/api">>}], MergedRepos).


repo_merging(_Config) ->
    Repo1 = #{name => <<"repo-1">>,
              api_url => <<"repo-1/api">>},
    Repo2 = #{name => <<"repo-2">>,
              repo_url => <<"repo-2/repo">>,
              repo_verify => false},
    Result = rebar_pkg_resource:merge_repos([Repo1, Repo2,
                                             #{name => <<"repo-2">>,
                                               api_url => <<"repo-2/api">>,
                                               repo_url => <<"bad url">>,
                                               repo_verify => true},
                                             #{name => <<"repo-1">>,
                                               api_url => <<"bad url">>,
                                               repo_verify => true},
                                             #{name => <<"repo-2">>,
                                               organization => <<"repo-2-org">>,
                                               api_url => <<"repo-2/api-2">>,
                                               repo_url => <<"other/repo">>}]),
    ?assertMatch([#{name := <<"repo-1">>,
                    api_url := <<"repo-1/api">>,
                    repo_verify := true},
                  #{name := <<"repo-2">>,
                    api_url := <<"repo-2/api">>,
                    repo_url := <<"repo-2/repo">>,
                    organization := <<"repo-2-org">>,
                    repo_verify := false}], Result).
