-module(tables_tests).
-include_lib("eunit/include/eunit.hrl").

-export([test1/0]).


test1() ->
    T0=tables:init(),
    {ok,_,T1}=tables:insert(#{n=>element1,a=>1,b=>2}, T0),
    {ok,_,T2}=tables:insert(#{n=>element2,a=>1,c=>2}, T1),
    io:format("T2 ~p~n",[T2]),
    {ok,T3}=tables:addindex([a,b,d],T2),
    io:format("T3 ~p~n",[T3]),
    {ok,T4}=tables:update(n,element2,#{c=>10},T3),
    io:format("T4 ~p~n",[T4]),
    {ok,T5}=tables:update(n,element2,#{a=>2},T4),
    io:format("T5 ~p~n",[T5]),
    ok.

all_test_() ->
    T0=tables:init(#{index=>[a,b],unique=>[n]}),
    {ok,ID1,T1}=tables:insert(#{n=>element1,a=>1,b=>2}, T0),
    {ok,_,T2}=tables:insert(#{n=>element2,a=>1,c=>2}, T1),
    {ok,F1}=tables:lookup(a,1,T2), %Index lookup
    {ok,F2}=tables:lookup(b,1,T2), %not found by index
    {ok,F3}=tables:lookup(c,2,T2), %seq. scan
    {ok,F4}=tables:lookup(x,1,T2), %not found by seq. scan
    T3=tables:del(ID1,T2),
    {ok,F5}=tables:lookup(a,1,T3), %Index lookup after delete
    {ok,F6}=tables:lookup(n,element2,T3), 
    {ok,T4}=tables:update(n,element2,#{c=>10},T3),
    {ok,F7}=tables:lookup(n,element2,T4), 
    [
    ?_assert(lists:sort([maps:get(n,E) || E<-F1]) =:= [element1,element2]),
    ?_assert(lists:sort([maps:get(n,E) || E<-F2]) =:= []),
    ?_assert(lists:sort([maps:get(n,E) || E<-F3]) =:= [element2]),
    ?_assert(lists:sort([maps:get(n,E) || E<-F4]) =:= []),
    ?_assert(lists:sort([maps:get(n,E) || E<-F5]) =:= [element2]),
    ?_assert(maps:get(c,hd(F6)) =:= 2),
    ?_assert(maps:get(c,hd(F7)) =:= 10)
    ].

