-module(worker_pool).

%% Public API
-export([
    new/1,
    take/1,
    return/2
]).

% [
%     Size, NextTake, NextReturn,
%     WorkerId1, WorkerId2, ...
% ]

-define(SIZE_POS, 1).
-define(TAKE_POS, 2).
-define(RETURN_POS, 3).
-define(HEADER_SIZE, 3).

-spec new(pos_integer()) -> atomics:atomics_ref().
new(Size) ->
    Ref = atomics:new(Size + ?HEADER_SIZE, []),
    atomics:put(Ref, ?SIZE_POS, Size),
    [atomics:put(Ref, Id + ?HEADER_SIZE, Id) || Id <- lists:seq(1, Size)],
    Ref.

-spec take(atomics:atomics_ref()) -> pos_integer() | empty.
take(Ref) ->
    NextTake = atomics:get(Ref, ?TAKE_POS),
    case atomics:exchange(Ref, NextTake + ?HEADER_SIZE + 1, 0) of
        0 ->
            empty;
        Id ->
            case NextTake + 1 < atomics:get(Ref, ?SIZE_POS) of
                true ->
                    atomics:put(Ref, ?TAKE_POS, NextTake + 1);
                _ ->
                    atomics:put(Ref, ?TAKE_POS, 0)
            end,
            Id
    end.

-spec return(atomics:atomics_ref(), pos_integer()) -> NeedNotify :: boolean().
return(Ref, Id) ->
    Size = atomics:get(Ref, ?SIZE_POS),
    NextReturn = atomics:add_get(Ref, ?RETURN_POS, 1),
    NextReturn =:= Size andalso atomics:sub(Ref, ?RETURN_POS, Size),
    ReturnPos = ((NextReturn - 1) rem Size),
    atomics:put(Ref, ReturnPos + ?HEADER_SIZE + 1, Id),
    atomics:get(Ref, ?TAKE_POS) =:= ReturnPos.

