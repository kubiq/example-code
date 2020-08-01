import { HttpErrorResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import * as moment from 'moment';
import { of } from 'rxjs';
import { catchError, delay, distinctUntilChanged, exhaustMap, map, switchMap, take, tap } from 'rxjs/operators';
import { none, option } from 'ts-option';

import { Api, IPaginatedResponse } from '../../../../api.service';
import { EventTypeService } from '../../../../eventtype.service';
import { JsonFormatService } from '../../../../general/helpers/json-format.service';
import { ToastService } from '../../../../general/toast/toast.service';
import {
    EventExistenceFlag,
    ExtractEvent,
    RequestHistoryEventPayload,
    RequestHistoryEventType,
} from '../../../../models';
import {
    EventBlockPatternCreate,
    EventBlockPatternsActions,
} from '../../../events/store/branches/block-patterns/block-patterns.actions';
import { EventBlockUrlActions } from '../../../events/store/branches/block-urls/block-urls.actions';
import {
    EventBlockActions,
} from '../../../events/store/branches/block/block.actions';
import {
    EventActions,
} from '../../../events/store/branches/event/event.actions';
import { EventService } from '../../../events/store/branches/event/event.service';
import { EventGlobalUrlActions } from '../../../events/store/branches/global-urls/global-url.actions';
import { RequestHistoryActions } from '../actions';
import { RequestHistoryService } from '../services';

@Injectable()
export class RequestHistoryEffectsService {

    requestHistoryLoad$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RequestHistoryActions.LoadHistory),
            exhaustMap(({payload: {skip, limit, id}}) =>
                this.requestHistoryService.get$().pipe(
                    take(1),
                    exhaustMap(({source, type}) => this.api.eventService.eventRequestHistory(id, skip, limit, source, type)),
                ),
            ),
            map(data => RequestHistoryActions.LoadRequestHistorySuccess(this.transformEvents(data))),
            catchError(({message}: HttpErrorResponse) => {
                this.toastService.danger('Could not load request history. Please try again in few minutes.');
                return of(RequestHistoryActions.LoadRequestHistoryError(message));
            }),
        ),
    );

    reload$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RequestHistoryActions.Reload),
            switchMap(() => this.requestHistoryService.get$().pipe(take(1))),
            switchMap(({skip, limit}) => this.eventService.get$().pipe(
                take(1), map((event) => RequestHistoryActions.LoadHistory({id: event.id, skip, limit}))),
            ),
        ),
    );

    notifyOutdated$ = createEffect(() =>
        this.actions$.pipe(
            ofType(
                EventActions.EventReload,
                EventActions.EventUpdateNoteSuccess,
                EventActions.EventUpdateSubTypeSuccess,
                EventActions.EventUpdateFlagSuccess,
                EventActions.EventPublish,
                EventGlobalUrlActions.EventCreateGlobalNegativeUrlSuccess,
                EventGlobalUrlActions.EventDeleteGlobalNegativeUrlSuccess,
                EventGlobalUrlActions.EventUpdateGlobalNegativeUrlSuccess,
                EventBlockUrlActions.EventUpdateBlockUrlNoteSuccess,
                EventBlockUrlActions.EventUpdateBlockUrlSuccess,
                EventBlockUrlActions.EventUpdateBlockUrlNote,
                EventBlockUrlActions.EventBlockUrlMoveToDifferentBlockSuccess,
                EventBlockActions.EventBlockCopySuccess,
                EventBlockActions.EventBlockDeleteSuccess,
                EventBlockActions.EventBlockUpdateTitleSuccess,
                EventBlockActions.EventBlockUpdateValidatorNoteSuccess,
                EventBlockActions.EventBlockUpdateAnalyseNoteSuccess,
                EventBlockPatternsActions.EventBlockPatternCreateSuccess,
                EventBlockPatternsActions.EventBlockPatternMoveToDifferentBlockSuccess,
                EventBlockPatternsActions.EventBlockPatternRemoveSuccess,
                EventBlockPatternsActions.EventBlockPatternRemoveManySuccess,
            ),
            map(() => RequestHistoryActions.NotifyOutdatedData()),
        ),
    );

    filterBySource$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RequestHistoryActions.SelectSource),
            switchMap(() => this.eventService.get$().pipe(
                take(1),
                map(({id}) => RequestHistoryActions.LoadHistory({id, skip: 0, limit: 10}))),
            ),
        ),
    );

    filterByType$ = createEffect(() =>
        this.actions$.pipe(
            ofType(RequestHistoryActions.SelectType),
            switchMap(() => this.eventService.get$().pipe(
                take(1),
                map(({id}) => RequestHistoryActions.LoadHistory({id, skip: 0, limit: 10}))),
            ),
        ),
    );

    closeHistoryOnEventChange$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EventActions.LoadEvent),
            distinctUntilChanged(((x, y) => x.payload === y.payload)),
            map(() => RequestHistoryActions.ShowHistory(false)),
        ),
    );

    constructor(
        private actions$: Actions,
        private api: Api,
        private toastService: ToastService,
        private jsonFormatService: JsonFormatService,
        private eventTypeService: EventTypeService, // todo use store
        private requestHistoryService: RequestHistoryService,
        private eventService: EventService,
    ) {
    }

    private transformEvents(response: IPaginatedResponse<RequestHistoryEventPayload>) {
        return {...response, items: response.items.map(evt => this.transformEvent(evt))};
    }

    private transformEvent<T extends RequestHistoryEventType>(event: ExtractEvent<RequestHistoryEventPayload, T>) {

        const doneBy = event.doneBy;
        const doneOn = moment(event.doneOn);
        const source = option(event.source);
        const ago = doneOn.toNow(true);

        const baseEvent = {
            ...event,
            doneOn,
            doneBy,
            source,
            ago,
        };

        if (isEventType(event, 'eventRequestAdded')) {
            return {
                ...baseEvent,
                type: event.type,
                domain: event.domain,
                eventType: event.eventType,
                eventSubtype: event.eventSubtype,
            };
        }

        if (isEventType(event, 'eventRequestEnabled')) {
            return {
                ...baseEvent,
                type: event.type,
            };
        }

        if (isEventType(event, 'eventRequestDisabled')) {
            return {
                ...baseEvent,
                type: event.type,
                message: option(event.message),
            };
        }

        if (isEventType(event, 'eventRequestExistenceUpdated')) {
            return {
                ...baseEvent,
                type: event.type,
                from: option(event.from).map(e => EventExistenceFlag[e]),
                to: EventExistenceFlag[event.to],
            };
        }

        if (isEventType(event, 'eventRequestTypeUpdated')) {
            return {
                ...baseEvent,
                type: event.type,
                from: option(event.from).map(f => this.eventTypeService.getTypeName(f)),
                fromSubtype: option(event.fromSubtype).map(f => this.eventTypeService.getSubtypeName(f)),
                to: this.eventTypeService.getTypeName(event.to),
                toSubtype: this.eventTypeService.getTypeName(event.toSubtype),
            };
        }

        if (isEventType(event, 'eventRequestBlockAdded')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: event.blockType,
                blockTitle: none,
            };
        }

        if (isEventType(event, 'eventRequestBlockRemoved')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: event.blockType,
            };
        }

        if (isEventType(event, 'eventRequestBlockTitleSet')) {
            return {
                ...baseEvent,
                type: event.type,
                blockTitle: option(event.blockTitle),
                blockType: option(event.blockType),
                from: option(event.from),
                to: event.to,
            };
        }

        if (isEventType(event, 'eventRequestBlockAnalystNoteSet')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: option(event.blockType),
                blockTitle: option(event.blockTitle),
                from: option(event.from),
                to: event.to,
            };
        }

        if (isEventType(event, 'eventRequestBlockValidatorNoteSet')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: option(event.blockType),
                blockTitle: option(event.blockTitle),
                from: option(event.from),
                to: event.to,
            };
        }

        if (isEventType(event, 'eventRequestBlockTransferred')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: option(event.blockType),
                blockTitle: option(event.blockTitle),
                from: option(event.from),
                to: option(event.to),
            };
        }

        if (isEventType(event, 'eventRequestBlockLabelsSet')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: option(event.blockType),
                blockTitle: option(event.blockTitle),
                from: option(event.from).map(l => l.join()),
                to: event.to.join(),
            };
        }

        if (isEventType(event, 'eventRequestBlockRangeSet')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: option(event.blockType),
                blockTitle: option(event.blockTitle),
                fromFrom: option(event.fromFrom).getOrElse('UNBOUND'),
                fromTo: option(event.fromTo).getOrElse('UNBOUND'),
                toFrom: option(event.toFrom).getOrElse('UNBOUND'),
                toTo: option(event.toTo).getOrElse('UNBOUND'),
            };
        }

        if (isEventType(event, 'eventRequestUrlAdded')) {
            return {
                ...baseEvent,
                type: event.type,
                blockId: event.blockId,
                blockType: option(event.blockType),
                blockTitle: option(event.blockTitle),
                url: event.url,
                urlType: option(event.urlType),
            };
        }

        if (isEventType(event, 'eventRequestGlobalUrlAdded')) {
            return {
                ...baseEvent,
                type: event.type,
                url: event.url,
                urlType: option(event.urlType),
            };
        }

        if (isEventType(event, 'eventRequestGlobalUrlValueSet')) {
            return {
                ...baseEvent,
                type: event.type,
                urlType: option(event.urlType),
                from: option(event.from),
                to: event.to,
            };
        }

        if (isEventType(event, 'eventRequestUrlRemoved')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: option(event.blockType),
                blockTitle: option(event.blockTitle),
                url: option(event.url),
                browser: option(event.browser),
                urlType: option(event.urlType),
            };
        }

        if (isEventType(event, 'eventRequestUrlValueSet')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: option(event.blockType),
                blockTitle: option(event.blockTitle),
                from: option(event.from),
                to: event.to,
            };
        }

        if (isEventType(event, 'eventRequestUrlNoteSet')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: option(event.blockType),
                from: option(event.from),
                to: event.to,
            };
        }

        if (isEventType(event, 'eventRequestUrlMoved')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: option(event.blockType),
                urlType: option(event.urlType),
                url: option(event.url),
                from: option(event.from),
                fromBlockType: option(event.fromBlockType),
                fromBlockTitle: option(event.fromBlockTitle),
                fromSource: option(event.fromSource),
                to: event.to,
                toBlockType: option(event.toBlockType),
                toBlockTitle: option(event.toBlockTitle),
                toSource: option(event.toSource),
            };
        }

        if (isEventType(event, 'eventRequestNoteUpdated')) {
            return {
                ...baseEvent,
                type: event.type,
                from: option(event.from),
                to: event.to,
            };
        }

        if (isEventType(event, 'eventRequestSubtypeUpdated')) {
            return {
                ...baseEvent,
                type: event.type,
                from: option(event.from),
                to: event.to,
            };
        }

        if (isEventType(event, 'eventRequestPatternAdded')) {
            return {
                ...baseEvent,
                type: event.type,
                pattern: this.jsonFormatService.beautifyJson(event.pattern),
                blockType: option(event.blockType),
                blockTitle: option(event.blockTitle),
            };
        }

        if (isEventType(event, 'eventRequestPatternUpdated')) {
            return {
                ...baseEvent,
                type: event.type,
                blockType: option(event.blockType),
                blockTitle: option(event.blockTitle),
                from: option(event.from).map(p => this.jsonFormatService.beautifyJson(p)),
                to: this.jsonFormatService.beautifyJson(event.to),
            };
        }

        if (isEventType(event, 'eventRequestPatternRemoved')) {
            return {
                ...baseEvent,
                type: event.type,
                pattern: option(event.pattern).map(p => this.jsonFormatService.beautifyJson(p)),
            };
        }

        if (isEventType(event, 'eventRequestAndRepositoryEventBound')) {
            return {
                ...baseEvent,
                type: event.type,
                kind: event.kind,
                repositoryEventId: event.repositoryEventId,
            };
        }

        if (isEventType(event, 'eventRequestAndRepositoryPatternBound')) {
            return {
                ...baseEvent,
                type: event.type,
                patternId: event.patternId,
                blockType: option(event.blockType),
                blockTitle: option(event.blockTitle),
                pattern: option(event.pattern).map(p => this.jsonFormatService.beautifyJson(p)),
                repositoryPatternId: event.repositoryPatternId,
            };
        }

        if (isEventType(event, 'eventRequestPatternMoved')) {
            return {
                ...baseEvent,
                type: event.type,
                id: event.id,
                pattern: option(event.pattern).map(p => this.jsonFormatService.beautifyJson(p)),
                from: option(event.from),
                fromBlockType: option(event.fromBlockType),
                fromBlockTitle: option(event.fromBlockTitle),
                fromSource: option(event.fromSource),
                to: event.to,
                toBlockType: option(event.toBlockType),
                toBlockTitle: option(event.toBlockTitle),
                toSource: option(event.toSource),
            };
        }

        const nah: never = event;
        return event;
    }
}

function isEventType<T extends RequestHistoryEventType>(event: RequestHistoryEventPayload, eventType: T)
    : event is ExtractEvent<RequestHistoryEventPayload, T> {
    return eventType === event.type;
}
