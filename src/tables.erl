-module(tables).

-export([init/0,init/1,insert/2,get/2,get/3,lookup/3,lookup/4,del/2,addindex/2]).

-type table() :: {'table',#{'data':=map(), 'indexes':=map(), 'last_write':=integer()}}.
-type id() :: integer().

-spec init() -> table().
init() ->
    init(#{}).

-spec init(map()) -> table().
init(Params) ->
    Indexes=lists:usort(maps:get(index,Params,[])++maps:get(unique,Params,[])),
    {table,#{
       data=>#{},
       indexes=>lists:foldl(fun(E,Acc)->maps:put(E,#{},Acc) end,#{},Indexes),
       unique=>lists:foldl(fun(E,Acc)->maps:put(E,true,Acc) end,#{},maps:get(unique,Params,[])),
       last_write=>erlang:system_time(),
       mkidfun=>maps:get(mkidfun,Params,fun new_id/0)
      }}.

-spec addindex(list(), table()) -> table().
addindex(NewIndexes, {table,#{data:=Data,indexes:=Index0}=Table}) ->
    Interest=['_id'|NewIndexes],
    Objects=maps:fold(
                 fun(_,V,Acc) ->
                         L=maps:with(NewIndexes,V),
                         if L==#{} -> Acc;
                            true -> [maps:with(Interest,V)|Acc]
                         end
                 end,[],Data),
    {table,
     Table#{indexes => 
       lists:foldl(
         fun(Key,Acc) ->
                 Idx=lists:foldl(
                   fun(E,IAcc) ->
                           try
                               Val=maps:get(Key,E),
                               maps:put(Val,[
                                             maps:get('_id',E)|
                                             maps:get(Val,IAcc,[])
                                            ],IAcc)
                       catch error:{badkey,Key} ->
                                 IAcc
                       end
                   end, #{}, Objects),
                 maps:put(Key,Idx,Acc)
         end, 
         Index0,
         NewIndexes)
      }}.


-spec insert(map(), table()) -> {ok,term(),table()}.
insert(Object, {table,#{data:=Data,indexes:=Index0,unique:=Uniq,mkidfun:=MkId}=Table}) ->
    ID=MkId(),
    NewData=maps:put(ID,Object#{'_id'=>ID},Data),
    NewIndex=lists:foldl(
               fun(Key, Indexes) ->
                       try
                           Val=maps:get(Key,Object),
                           KIdx=maps:get(Key,Indexes),
                           Exists=maps:get(Val,KIdx,[]),
                           if Exists==[] ->
                                  ok;
                              true ->
                                  case maps:get(Key,Uniq,false) of
                                      false -> ok;
                                      true ->
                                          throw({constraint,unique,Key})
                                  end
                           end,
                           NewKeyIdx=maps:put(Val,[ID|Exists],KIdx),
                           maps:put(Key,NewKeyIdx,Indexes)
                       catch error:{badkey,Key} ->
                                 Indexes
                       end
               end, Index0, maps:keys(Index0)),
    {ok,
     ID,
     {table,Table#{
       data=>NewData,
       indexes=>NewIndex,
       last_write=>erlang:system_time()
      }}
    }.

-spec get(id(), table()) -> {ok, map()} | notfound.
get(ID,{table,#{data:=Data}}) ->
    try
        {ok, maps:get(ID, Data) }
    catch error:{badkey,ID} ->
              notfound
    end.

-spec get(id(), table(), term()) -> map() | term().
get(ID,{table,#{data:=Data}}, Default) ->
    try
        maps:get(ID, Data)
    catch error:{badkey,ID} ->
              Default
    end.


-spec del(map()|id(), table()) -> table().
del(#{'_id':=ID},Table) ->
    del(ID,Table);

del(ID,{table,#{data:=Data,indexes:=Index0}=Table}) ->
    Object=maps:get(ID,Data),
    NewIndex=lists:foldl(
               fun(Key, Indexes) ->
                       try
                           Val=maps:get(Key,Object),
                           KIdx=maps:get(Key,Indexes),
                           Exists=maps:get(Val,KIdx),
                           NewKeyIdx=maps:put(Val,Exists--[ID],KIdx),
                           maps:put(Key,NewKeyIdx,Indexes)
                       catch error:{badkey,Key} ->
                                 io:format("BK ~p~n",[Key]),
                                 Indexes
                       end
               end, Index0, maps:keys(Index0)),
    {table, Table#{
       indexes=>NewIndex,
       data=>maps:remove(ID,Data),
       last_write=>erlang:system_time()
      }
    }.

-spec lookup(term(), term(), table()) -> {ok, [map()]}.
lookup(Field, Value, Table) ->
    lookup(Field, Value, Table, []).

-spec lookup(term(), term(), table(), list()) -> {ok, [map()]}.
lookup(Key, Value, {table,#{indexes:=Idx}=Table},Opts) ->
    case maps:is_key(Key, Idx) of
        false -> %sequence scan
            lookup_scan(Key,Value,Table,Opts);
        true -> %index lookup
            lookup_index(Key,Value,Table,Opts)
    end.

lookup_scan(Key, Value, #{data:=Data},_Opts) ->
    {ok,
     maps:fold(
       fun(_K,Element,Acc) ->
               try
                   Value=maps:get(Key,Element),
                   [Element|Acc]
               catch _:_ ->
                         Acc
               end
       end, [], Data)
    }.
    
lookup_index(Key, Value, #{indexes:=Idx,data:=Data}, _Opts) ->
    Index=maps:get(Key,Idx),
    IDs=maps:get(Value,Index,[]),
    {ok,
     lists:map(
       fun(ID) ->
               maps:get(ID,Data)
       end, IDs)
      }.

new_id() ->
    %Data = term_to_binary([make_ref(), erlang:system_time(), rand:uniform()]),
    %binary:decode_unsigned(crypto:hash(sha,Data)).
    %(crypto:hash(sha,Data)).
    erlang:unique_integer([monotonic,positive]).

