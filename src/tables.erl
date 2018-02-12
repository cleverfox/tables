-module(tables).

-export([init/0,
         init/1,
         init/2,
         insert/2,
         get/2,get/3,
         lookup/3,lookup/4,
         mlookup/3,
         del/2,
         delete/3,
         addindex/2,
         update/4,
         upsert/3,
         export/1,
         import/2,
         to_list/1
        ]).

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

-spec init(map(),table()) -> table().
init(Params, {table, Table}) ->
    {table,Table#{
       mkidfun=>maps:get(mkidfun,Params,fun new_id/0)
      }}.

-spec to_list(table()) -> list().
to_list({table, #{data:=Data}}) ->
    maps:values(Data).

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
    {ok,
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
      }}
    }.


-spec insert(map(), table()) -> {ok,term(),table()}.
insert(Object0, {table,#{data:=Data,indexes:=Index0,unique:=Uniq,mkidfun:=MkId}=Table}) ->
    {Object,ID}=case maps:get('_id',Object0,undefined) of
                    undefined ->
                        TID=MkId(),
                        {Object0#{'_id'=>TID}, TID};
                    TID ->
                        case maps:get(TID, Data, undefined) of
                            undefined ->
                                {Object0, TID};
                            _ ->
                                throw({constraint,unique,'_id',TID})
                        end
                end,
    NewData=maps:put(ID,Object, Data),
    NewIndex=lists:foldl(
               fun(Key, Indexes) when is_tuple(Key) -> % not working yet
                       throw("Multikey index not supported yet"),
                       FindObj=lists:foldl(
                                 fun(_,false) ->
                                         false;
                                    (IndexKey,AccObj) ->
                                         case maps:is_key(IndexKey,Object) of
                                             false ->
                                                 %null field exists...
                                                 false;
                                             true ->
                                                 maps:put(IndexKey,
                                                          maps:get(IndexKey,Object),
                                                          AccObj)
                                         end
                                 end,
                                 #{},
                                 erlang:tuple_to_list(Key)
                                ),
                       if FindObj==false ->
                              Indexes;
                          true ->
                              case mlookup(FindObj,{table,Table},[]) of
                                  {ok, []} ->
                                      Indexes;
                                  {ok, [Exists|_]} ->
                                      throw({constraint,unique,Key,hd(Exists)})
                              end
                       end;
                   (Key, Indexes) when is_atom(Key) ->
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
                                          throw({constraint,unique,Key,hd(Exists)})
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
                           NewLst=Exists--[ID],
                           if NewLst == [] ->
                                  NewKeyIdx=maps:remove(Val,KIdx),
                                  maps:put(Key,NewKeyIdx,Indexes);
                              true ->
                                  NewKeyIdx=maps:put(Val,NewLst,KIdx),
                                  maps:put(Key,NewKeyIdx,Indexes)
                           end
                       catch error:{badkey,Key} ->
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

-spec mlookup(map(), table(), list()) -> {ok, [map()]}.
mlookup(Map, {table,#{indexes:=Idx}=Table},Opts) ->
    Keys=lists:reverse(
          lists:keysort(1,
                        maps:fold(
                          fun(K,_V,Acc) ->
                                  case maps:is_key(K, Idx) of
                                      true ->
                                          [{maps:size(maps:get(K,Idx)),K}|Acc];
                                      false ->
                                          Acc
                                  end
                          end, [], Map)
                       )
         ),
    {ok,Matches}= case Keys of
        [] -> %sequence scan
            throw('no_keys');
        [{_,Key}|_] -> %index lookup
            lookup_index(Key,maps:get(Key,Map),Table,Opts)
    end,
    {ok,
    lists:filter(
      fun(Element) ->
              maps:fold(
                fun(_,_,false) -> false;
                   (MKey,MVal,true) ->
                        maps:get(MKey,Element,undefined)==MVal
                end, true, Map)
      end,
      Matches)
    }.


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

upsert(Key, NewObject, {table,#{data:=Data,indexes:=Idx}=Table}) ->
    {ok, Matched}=lookup(Key, maps:get(Key,NewObject), {table,Table},[]),
    case Matched of 
        [Object] ->
            New=maps:merge(Object,NewObject),
            FullUpdate=lists:foldl(
                         fun (_,true) -> 
                                 true;
                             ('_id', _) ->
                                 maps:get('_id',Object)=/=maps:get('_id',New);
                             (Key1, _) ->
                                 maps:get(Key1,Object,undefined)=/=maps:get(Key1,New,undefined)
                         end,
                         false,
                         maps:keys(Idx)),
            if FullUpdate -> %index affected. full update
                   T2=del(Object,{table,Table}),
                   {ok, NewID, T3}=insert(New,T2),
                   {ok, NewID, T3};
               true -> %index not affected
                   NewID=maps:get('_id',Object),
                   {ok, NewID, {table, 
                                Table#{ data => maps:put( NewID, New, Data) }
                               }
                   }
            end;
        [] ->
            insert(NewObject,{table,Table});
        [_|_] ->
            throw({matched,multiple,Key})
    end.


delete(Key, Value, {table,_}=TTable) ->
    {ok, Matched}=lookup(Key, Value, TTable,[]),
    TTable1=lists:foldl(
             fun(Object,Acc) ->
                     del(Object,Acc)
             end, TTable, Matched),
    {ok, TTable1}.


update(Key, Value, NewObject, {table,#{indexes:=Idx}=Table}) ->
    {ok, Matched}=lookup(Key, Value, {table,Table},[]),
    Table1=lists:foldl(
      fun(Object,Acc) ->
              New=maps:merge(Object,NewObject),
              if Object == New ->
                     Acc;
                 true ->
                     ChangeKeys=maps:keys(NewObject),
                     FullUpdate=lists:foldl(
                                  fun('_id', _) ->
                                          true;
                                     (_,true) -> 
                                          true;
                                     (Key1, false) ->
                                          maps:is_key(Key1,Idx)
                                  end,
                                  false,
                                  ChangeKeys),
                     if FullUpdate -> %index affected. full update
                            T2=del(Object,{table,Acc}),
                            {ok, _, {table, T3}}=insert(New,T2),
                            T3;
                        true -> %index not affected
                            Acc#{
                              data => maps:put(
                                        maps:get('_id',Object),
                                        New,
                                        maps:get(data,Acc)
                                       )
                             }
                     end
              end
      end, Table, Matched),
    {ok, {table, Table1}}.

export({table, #{indexes:=Idx,unique:=Uniq,data:=Data}=_Table}) ->
    term_to_binary({tabledump, #{ 
                      v=>1,
                      idx=>maps:keys(Idx),
                      uniq=>maps:keys(Uniq),
                      data=>maps:values(Data)
                     }}).

import(Payload,{table, #{indexes:=Idx0,unique:=Uniq0}=Table}) ->
    {tabledump, #{
       v:=Ver,
       idx:=Idx1,
       uniq:=Uniq1,
       data:=Data
      }}=binary_to_term(Payload),
    Ver=1,
    Unique=lists:usort(maps:keys(Uniq0)++Uniq1),
    Indexes=lists:usort(maps:keys(Idx0)++Idx1++Unique),
    T1=lists:foldl(fun(Object,Acc) ->
                       {ok,_,Acc2}=insert(Object,Acc),
                       Acc2
                end, {table, Table#{indexes=>#{}, unique=>#{}}}, Data),
    {ok,T2}=addindex(Indexes,T1),
    adduniq(Unique,T2).

adduniq(U,{table, #{indexes:=Index, unique:=U0}=Table}) ->
    lists:foreach(
      fun(IndexName) ->
              maps:fold(
                fun(_,[_],_) ->
                        ok;
                   (Key,[_|_],_) ->
                        throw({constraint,unique,IndexName,Key})
                end, ok, maps:get(IndexName, Index))
      end, U),
    {table, Table#{ 
               unique=>lists:foldl(fun(E,Acc)->maps:put(E,true,Acc) end,U0,U)
             }}.
    


new_id() ->
    %Data = term_to_binary([make_ref(), erlang:system_time(), rand:uniform()]),
    %binary:decode_unsigned(crypto:hash(sha,Data)).
    erlang:unique_integer([monotonic,positive]).

