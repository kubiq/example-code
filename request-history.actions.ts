import { createAction, on } from '@ngrx/store';

import { IPaginatedResponse } from '../../../../api.service';
import { IHistoryFilter, IPaginatedRequest, RequestHistoryEventRich } from '../../../../models';
import {
    createReducerList,
    defaultErrorHandler,
    PayloadPrepare,
    stateErrorHandler,
} from '../../../events/store/utils';
import { RequestHistoryState } from '../state.store';

export const actionDescriber = '[Request History component]';

export const ShowHistory = createAction(`${actionDescriber} visible`, PayloadPrepare<boolean>());
export const LoadHistory = createAction(`${actionDescriber} load`, PayloadPrepare<IPaginatedRequest>());
export const LoadRequestHistorySuccess = createAction(
    `${actionDescriber} success`,
    PayloadPrepare<IPaginatedResponse<RequestHistoryEventRich>>());
export const LoadRequestHistoryError = createAction(`${actionDescriber} fail`, defaultErrorHandler());
export const Reload = createAction(`${actionDescriber} reload`);
export const SelectSource = createAction(
    `${actionDescriber} filter by source`,
    PayloadPrepare<IHistoryFilter['source']>());
export const SelectType = createAction(`${actionDescriber} filter by type`, PayloadPrepare<IHistoryFilter['type']>());
export const NotifyOutdatedData = createAction(`${actionDescriber} notify outdated`);

export const RequestHistoryActions = {
    ShowHistory,
    LoadHistory,
    LoadRequestHistorySuccess,
    LoadRequestHistoryError,
    Reload,
    SelectSource,
    SelectType,
    NotifyOutdatedData,
};

export const MainRequestHistoryReducerCollection = createReducerList<RequestHistoryState>(
    on(RequestHistoryActions.ShowHistory, (store, {payload: visible}) => ({...store, visible, items: []})),
    on(RequestHistoryActions.LoadHistory, (store) => ({...store, loading: true, error: null})),
    on(RequestHistoryActions.LoadRequestHistorySuccess, (store, {payload}) => {
        return {...store, ...payload, loading: false, error: null, visible: true, outdatedData: false};
    }),
    on(RequestHistoryActions.LoadRequestHistoryError, stateErrorHandler),
    on(
        RequestHistoryActions.Reload,
        (data) => ({...data, loading: true, error: null, outdatedData: false})),
    on(RequestHistoryActions.NotifyOutdatedData, (store) => ({...store, outdatedData: store.visible})),
    on(RequestHistoryActions.SelectSource, (store, {payload: source}) => ({...store, source})),
    on(RequestHistoryActions.SelectType, (store, {payload: type}) => ({...store, type})),
);
