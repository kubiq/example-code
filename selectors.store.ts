import { createFeatureSelector, createSelector } from '@ngrx/store';
import { none, option, Option } from 'ts-option';

import { RequestHistoryState } from './state.store';

const eventStateFeatureSelector = createFeatureSelector<RequestHistoryState>('history');

export const getRequestHistoryLoading = createSelector(eventStateFeatureSelector, ({ loading }) => loading);
export const getRequestHistoryVisible = createSelector(eventStateFeatureSelector, ({ visible }) => visible);
export const getRequestHistoryError = createSelector(eventStateFeatureSelector, ({ error }) => error);

export const getRequestHistoryState = createSelector(eventStateFeatureSelector, state => {
    const { loading, error, ...rest } = state;
    return rest;
});
export const getHistoryItems = createSelector(getRequestHistoryState, ({ items }) => items);
export const getCurrentPage = createSelector(getRequestHistoryState, ({ skip, limit }) => (skip / limit) + 1);
export const getTotalItems = createSelector(getRequestHistoryState, ({ count }) => count);
export const getItemsPerPage = createSelector(getRequestHistoryState, ({ limit }) => limit);
export const getOutdatedData = createSelector(getRequestHistoryState, ({ outdatedData }) => outdatedData);
export const getSelectedSource = createSelector(getRequestHistoryState, ({ source }) => source);
export const getSelectedType = createSelector(getRequestHistoryState, ({ type }) => type);
export const getWarningMessage = createSelector(getRequestHistoryState, ({ items, source, type }): Option<string> => {

    if (items && items.length === 0) {
        if (source === 'all' && type === 'all') {
            return option('History is empty for this event');
        }
        if (source !== 'all') {
            return option('No history items found for this source');
        }
        if (type !== 'all') {
            return option('No history items found for this event type');
        }
    }

    return none;
});
