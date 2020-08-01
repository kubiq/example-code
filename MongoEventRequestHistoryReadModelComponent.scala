package com.jumpshot.patternmanagement.scalamongo

import java.time.{Instant, LocalDate}
import java.util.UUID

import org.bson.codecs.Codec // do not remove, Macros depend on it
import com.jumpshot.mongocommon.scalamongo.PolymorphicCodecProvider
import com.jumpshot.patternmanagement.EventRepositoryBindingKind
import com.jumpshot.patternmanagement.event.EventRequestBlockTransferred.{PatternTransfer, UrlTransfer}
import com.jumpshot.patternmanagement.event.GenericHandleableEvent
import com.jumpshot.patternmanagement.execution.ExecutionContextProvider
import com.jumpshot.patternmanagement.model._
import com.jumpshot.patternmanagement.event._
import com.jumpshot.patternmanagement.model.EventRequestHistoryReadModel.EventHistoryItems._
import com.typesafe.scalalogging.StrictLogging
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.codecs.Macros
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Filters.equal

import scala.async.Async._
import scala.concurrent.Future
import scala.reflect.ClassTag

trait MongoEventRequestHistoryReadModelComponent extends EventRequestHistoryReadModelComponent
  with ExecutionContextProvider.Implicit {

  this: ScalaMongoDbProvider
    with StrictLogging
    with ExecutionContextProvider =>

  override def eventHistoryReadModel: EventRequestHistoryReadModel = new MongoEventRequestHistoryReadModel

  class MongoEventRequestHistoryReadModel extends EventRequestHistoryReadModel {

    import MongoEventRequestHistoryReadModel._
    import EventUpgradeImplicits._
    import implicits._

    override def handle: PartialFunction[GenericHandleableEvent, Future[Unit]] = {

      case HandleableEvent(evt: EventRequestAdded) => handleEventRequestAdded(evt)
      case HandleableEvent(evt: EventRequestAddedV2) => handleEventRequestAdded(evt)
      case HandleableEvent(evt: EventRequestEnabled) => handleEventRequestEnabled(evt)
      case HandleableEvent(evt: EventRequestDisabled) => handleEventRequestDisabled(evt)
      case HandleableEvent(evt: EventRequestTypeUpdated) => handleEventRequestTypeUpdated(evt)

      case HandleableEvent(evt: EventRequestExistenceUpdated) => handleEventRequestExistenceUpdated(evt)
      case HandleableEvent(evt: EventRequestExistenceUpdatedV2) => handleEventRequestExistenceUpdated(evt)

      case HandleableEvent(evt: EventRequestBlockAdded) => handleEventRequestBlockAdded(evt)
      case HandleableEvent(evt: EventRequestBlockAddedV2) => handleEventRequestBlockAdded(evt)
      case HandleableEvent(evt: EventRequestBlockRemoved) => handleEventRequestBlockRemoved(evt)
      case HandleableEvent(evt: EventRequestBlockNoteSet) => handleEventRequestBlockNoteSet(evt)
      case HandleableEvent(evt: EventRequestBlockAnalystNoteSet) => handleEventRequestBlockAnalystNoteSet(evt)
      case HandleableEvent(evt: EventRequestBlockValidatorNoteSet) => handleEventRequestBlockValidatorNoteSet(evt)
      case HandleableEvent(evt: EventRequestBlockTransferred) => handleEventRequestBlockTransferred(evt)
      case HandleableEvent(evt: EventRequestBlockTitleSet) => handleEventRequestBlockTitleSet(evt)
      case HandleableEvent(evt: EventRequestBlockLabelsSet) => handleEventRequestBlockLabelsSet(evt)
      case HandleableEvent(evt: EventRequestBlockRangeSet) => handleEventRequestBlockRangeSet(evt)

      case HandleableEvent(evt: EventRequestGlobalUrlAdded) => handleEventRequestGlobalUrlAdded(evt)
      case HandleableEvent(evt: EventRequestGlobalUrlValueSet) => handleEventRequestGlobalUrlValueSet(evt)
      case HandleableEvent(evt: EventRequestGlobalUrlRemoved) => handleEventRequestGlobalUrlRemoved(evt)
      case HandleableEvent(evt: EventRequestGlobalUrlNoteSet) => handleEventRequestGlobalUrlNoteSet(evt)

      case HandleableEvent(evt: EventRequestUrlAdded) => handleEventRequestBlockUrlAdded(evt)
      case HandleableEvent(evt: EventRequestUrlAddedV2) => handleEventRequestBlockUrlAdded(evt)
      case HandleableEvent(evt: EventRequestUrlAddedV3) => handleEventRequestBlockUrlAdded(evt)
      case HandleableEvent(evt: EventRequestUrlValueSet) => handleEventRequestBlockUrlValueSet(evt)
      case HandleableEvent(evt: EventRequestUrlRemoved) => handleEventRequestBlockUrlRemoved(evt)
      case HandleableEvent(evt: EventRequestUrlNoteSet) => handleEventRequestUrlNoteSet(evt)
      case HandleableEvent(evt: EventRequestUrlMoved) => handleEventRequestUrlMoved(evt)

      case HandleableEvent(evt: EventRequestNoteUpdated) => handleEventRequestNoteUpdated(evt)
      case HandleableEvent(evt: EventRequestNoteUpdatedV2) => handleEventRequestNoteUpdated(evt)

      case HandleableEvent(evt: EventRequestSubtypeUpdated) => handleEventRequestSubtypeUpdated(evt)

      case HandleableEvent(evt: EventRequestPatternAdded) => handleEventRequestPatternAdded(evt)
      case HandleableEvent(evt: EventRequestPatternUpdated) => handleEventRequestPatternUpdated(evt)
      case HandleableEvent(evt: EventRequestPatternRemoved) => handleEventRequestPatternRemoved(evt)

      case HandleableEvent(evt: EventRequestAndRepositoryEventBound) => handleEventRequestRepositoryEventBound(evt)
      case HandleableEvent(evt: EventRequestAndRepositoryEventBoundV2) => handleEventRequestRepositoryEventBound(evt)
      case HandleableEvent(evt: EventRequestAndRepositoryEventBoundV3) => handleEventRequestRepositoryEventBound(evt)

      case HandleableEvent(evt: EventRequestAndRepositoryPatternBound) => handleEventRequestRepositoryPatternBound(evt)
      case HandleableEvent(evt: EventRequestPatternMoved) => handleEventRequestPatternMoved(evt)

    }

    private def handleEventRequestAdded(evt: EventRequestAdded): Future[Unit] = {

      val request = BsonEventRequestAdded(
        evt.requestId,
        evt.domain,
        evt.doneBy,
        evt.doneOn.toInstant,
        evt.eventTypeId,
        evt.eventSubtypeId
      )

      eventRequestHistoryCollection flatMap (_.insertOne(request))
    }

    private def handleEventRequestEnabled(evt: EventRequestEnabled): Future[Unit] = {

      val request = BsonEventRequestEnabled(
        evt.requestId,
        evt.doneBy,
        evt.doneOn.toInstant,
      )

      eventRequestHistoryCollection flatMap (_.insertOne(request))
    }

    private def handleEventRequestDisabled(evt: EventRequestDisabled): Future[Unit] = {

      val request = BsonEventRequestDisabled(
        evt.requestId,
        Option(evt.message),
        evt.doneBy,
        evt.doneOn.toInstant,
      )

      eventRequestHistoryCollection flatMap (_.insertOne(request))
    }

    private def handleEventRequestTypeUpdated(evt: EventRequestTypeUpdated): Future[Unit] = async {
      val maybePreviousEvent = await (findPreviousOperation[BsonEventRequestTypeUpdated](Selectors.byRequestId(evt.requestId)))
      val maybeAddEvent = await (findPreviousOperation[BsonEventRequestAdded](Selectors.byRequestId(evt.requestId)))

      val request = BsonEventRequestTypeUpdated(
        evt.requestId,
        maybeAddEvent.flatMap(_.source),
        maybePreviousEvent.map(_.to),
        evt.eventType,
        evt.doneBy,
        evt.doneOn.toInstant,
      )

      await (eventRequestHistoryCollection flatMap (_.insertOne(request)))
    }

    private def handleEventRequestExistenceUpdated(evt: EventRequestExistenceUpdatedV2): Future[Unit] = async {
      val maybePreviousEvent = await (findPreviousOperation[BsonEventRequestExistenceUpdated](Selectors.byRequestIdAndSource(evt.requestId, evt.source)))

      val operation = BsonEventRequestExistenceUpdated(
        evt.requestId,
        Some(evt.source),
        maybePreviousEvent.map(_.to),
        evt.existence,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestBlockAdded(evt: EventRequestBlockAddedV2): Future[Unit] = {

      val operation = BsonEventRequestBlockAdded(
        evt.requestId,
        evt.id,
        Some(evt.blockSource),
        evt.blockType,
        evt.doneBy,
        evt.doneOn.toInstant)

      eventRequestHistoryCollection flatMap (_.insertOne(operation))
    }

    private def handleEventRequestBlockRemoved(evt: EventRequestBlockRemoved): Future[Unit] = async {
      val blockInfo = await(getBlockInfo(evt.id))

      val operation = BsonEventRequestBlockRemoved(
        evt.requestId,
        blockInfo.source,
        blockInfo.blockType,
        blockInfo.blockTitle,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestBlockNoteSet(evt: EventRequestBlockNoteSet): Future[Unit] = async {
      val maybePreviousEvent = await (findPreviousOperation[BsonEventRequestBlockValidatorNoteSet](Selectors.byBlockId(evt.blockId)))
      val blockInfo = await(getBlockInfo(evt.blockId))

      val operation = BsonEventRequestBlockNoteSet(
        evt.requestId,
        Some(evt.blockId),
        blockInfo.blockType,
        blockInfo.source,
        blockInfo.blockTitle,
        maybePreviousEvent.map(_.to),
        evt.note,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestBlockAnalystNoteSet(evt: EventRequestBlockAnalystNoteSet): Future[Unit] = async {
      val maybePreviousEvent = await (findPreviousOperation[BsonEventRequestBlockAnalystNoteSet](Selectors.byBlockId(evt.blockId)))
      val blockInfo = await(getBlockInfo(evt.blockId))

      val operation = BsonEventRequestBlockAnalystNoteSet(
        evt.requestId,
        blockInfo.source,
        Some(evt.blockId),
        blockInfo.blockType,
        blockInfo.blockTitle,
        maybePreviousEvent.map(_.to),
        evt.note,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestBlockValidatorNoteSet(evt: EventRequestBlockValidatorNoteSet): Future[Unit] = async {
      val maybePreviousEvent = await (findPreviousOperation[BsonEventRequestBlockValidatorNoteSet](Selectors.byBlockId(evt.blockId)))
      val blockInfo = await(getBlockInfo(evt.blockId))

      val operation = BsonEventRequestBlockValidatorNoteSet(
        evt.requestId,
        Some(evt.blockId),
        blockInfo.source,
        blockInfo.blockType,
        blockInfo.blockTitle,
        maybePreviousEvent.map(_.to),
        evt.note,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestBlockTransferred(evt: EventRequestBlockTransferred): Future[Unit] = async {
      val blockInfo = await(getBlockInfo(evt.payload.originalId))

      val urls = evt.payload.urls.map(_.id)
      val patterns = evt.payload.patterns.map(_.id)

      val operation = BsonEventRequestBlockTransferred(
        evt.requestId,
        evt.payload.id,
        blockInfo.source,
        evt.payload.blockType,
        blockInfo.blockTitle,
        urls,
        evt.payload.urls,
        patterns,
        evt.payload.patterns,
        blockInfo.source,
        evt.payload.blockSource,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestBlockTitleSet(evt: EventRequestBlockTitleSet): Future[Unit] = async {
      val blockInfo = await(getBlockInfo(evt.blockId))

      val operation = BsonEventRequestBlockTitleSet(
        evt.requestId,
        evt.blockId,
        blockInfo.source,
        blockInfo.blockType,
        blockInfo.blockTitle,
        evt.title,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestBlockLabelsSet(evt: EventRequestBlockLabelsSet): Future[Unit] = async {
      val maybePreviousEvent = await (findPreviousOperation[BsonEventRequestBlockLabelsSet](Selectors.byBlockId(evt.blockId)))
      val blockInfo = await(getBlockInfo(evt.blockId))

      val operation = BsonEventRequestBlockLabelsSet(
        evt.requestId,
        blockInfo.source,
        evt.blockId,
        blockInfo.blockType,
        blockInfo.blockTitle,
        maybePreviousEvent.map(_.to),
        evt.labels,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestBlockRangeSet(evt: EventRequestBlockRangeSet): Future[Unit] = async {
      val maybePreviousEvent = await (findPreviousOperation[BsonEventRequestBlockRangeSet](Selectors.byBlockId(evt.blockId)))
      val blockInfo = await(getBlockInfo(evt.blockId))

      val operation = BsonEventRequestBlockRangeSet(
        evt.requestId,
        blockInfo.source,
        evt.blockId,
        blockInfo.blockType,
        blockInfo.blockTitle,
        maybePreviousEvent.flatMap(_.toFrom),
        maybePreviousEvent.flatMap(_.toTo),
        evt.from,
        evt.to,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestGlobalUrlAdded(evt: EventRequestGlobalUrlAdded): Future[Unit] = {

      val operation = BsonEventRequestGlobalUrlAdded(
        evt.requestId,
        evt.id,
        source = None,
        evt.urlType,
        evt.browser,
        evt.url,
        evt.doneBy,
        evt.doneOn.toInstant)

      eventRequestHistoryCollection flatMap (_.insertOne(operation))
    }

    private def handleEventRequestGlobalUrlRemoved(evt: EventRequestGlobalUrlRemoved): Future[Unit] = async {
      val maybePreviousUpdateEvent = await (findPreviousOperation[BsonEventRequestUrlValueSet](Selectors.byRequestId(evt.requestId)))
      val maybePreviousAddEvent = await (findPreviousOperation[BsonEventRequestGlobalUrlAdded](Selectors.byRequestId(evt.requestId)))

      val operation = BsonEventRequestUrlRemoved(
        evt.requestId,
        evt.id,
        maybePreviousUpdateEvent.flatMap(_.source).orElse(maybePreviousAddEvent.flatMap(_.source)),
        blockId = None,
        blockType = None,
        blockTitle = None,
        maybePreviousUpdateEvent.flatMap(_.urlType),
        maybePreviousUpdateEvent.flatMap(_.browser),
        maybePreviousUpdateEvent.map(_.to),
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestGlobalUrlNoteSet(evt: EventRequestGlobalUrlNoteSet): Future[Unit] = async {
      val maybePreviousEvent = await (findPreviousOperation[BsonEventRequestUrlNoteSet](Selectors.byUrlId(evt.id)))
      val maybeAddUrlEvent = await (findPreviousOperation[BsonEventRequestGlobalUrlAdded](Selectors.byUrlId(evt.id)))

      val operation = BsonEventRequestUrlNoteSet(
        evt.requestId,
        evt.id,
        maybeAddUrlEvent.flatMap(_.source),
        blockId = None,
        blockType = None,
        blockTitle = None,
        maybePreviousEvent.flatMap(_.urlType),
        maybePreviousEvent.flatMap(_.browser),
        maybePreviousEvent.map(_.to),
        evt.note,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestGlobalUrlValueSet(evt: EventRequestGlobalUrlValueSet): Future[Unit] = async {
      val maybePreviousEvent = await (findPreviousOperation[BsonEventRequestGlobalUrlValueSet](Selectors.byUrlId(evt.id)))
      val maybePreviousAddEvent = await (findPreviousOperation[BsonEventRequestGlobalUrlAdded](Selectors.byUrlId(evt.id)))

      val operation = BsonEventRequestGlobalUrlValueSet(
        evt.requestId,
        evt.id,
        maybePreviousAddEvent.flatMap(_.source),
        maybePreviousAddEvent.flatMap(_.browser),
        maybePreviousAddEvent.map(_.urlType),
        maybePreviousEvent.map(_.newUrl),
        evt.url,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestBlockUrlAdded(evt: EventRequestUrlAddedV3): Future[Unit] = async {
      val blockInfo = await(getBlockInfo(evt.blockId))

      val operation = BsonEventRequestUrlAdded(
        evt.requestId,
        evt.id,
        blockInfo.source,
        evt.blockId,
        blockInfo.blockType,
        blockInfo.blockTitle,
        evt.urlType,
        evt.browser,
        evt.url,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestBlockUrlValueSet(evt: EventRequestUrlValueSet): Future[Unit] = async {
      val urlInfo = await(getUrlInfo(evt.id))

      val operation = BsonEventRequestUrlValueSet(
        evt.requestId,
        evt.id,
        urlInfo.source,
        urlInfo.blockId,
        urlInfo.blockType,
        urlInfo.blockTitle,
        urlInfo.browser,
        urlInfo.urlType,
        urlInfo.url,
        evt.url,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestBlockUrlRemoved(evt: EventRequestUrlRemoved): Future[Unit] = async {
      val urlInfo = await(getUrlInfo(evt.id))

      val operation = BsonEventRequestUrlRemoved(
        evt.requestId,
        evt.id,
        urlInfo.source,
        urlInfo.blockId,
        urlInfo.blockType,
        urlInfo.blockTitle,
        urlInfo.urlType,
        urlInfo.browser,
        urlInfo.url,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestUrlNoteSet(evt: EventRequestUrlNoteSet): Future[Unit] = async {
      val maybePreviousEvent = await (findPreviousOperation[BsonEventRequestUrlNoteSet](Selectors.byUrlId(evt.id)))

      val urlInfo = await(getUrlInfo(evt.id))

      val operation = BsonEventRequestUrlNoteSet(
        evt.requestId,
        evt.id,
        urlInfo.source,
        urlInfo.blockId,
        urlInfo.blockType,
        urlInfo.blockTitle,
        urlInfo.urlType,
        urlInfo.browser,
        maybePreviousEvent.map(_.to),
        evt.note,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestUrlMoved(evt: EventRequestUrlMoved): Future[Unit] = async {

      val urlInfo = await(getUrlInfo(evt.id))

      val toBlockInfo: BlockInfo = await(getBlockInfo(evt.targetBlockId))

      val operation = BsonEventRequestUrlMoved(
        evt.requestId,
        evt.id,
        urlInfo.url,
        urlInfo.urlType,
        urlInfo.browser,
        urlInfo.blockId,
        urlInfo.blockType,
        urlInfo.blockTitle,
        urlInfo.source,
        evt.targetBlockId,
        toBlockInfo.blockType,
        toBlockInfo.blockTitle,
        toBlockInfo.source,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestUrlBrowserSet(evt: EventRequestUrlBrowserSet): Future[Unit] = async {
      val maybePreviousEvent = await (findPreviousOperation[BsonEventRequestUrlBrowserSet](Selectors.byId(evt.id)))
      val maybeBlockUrlSetEvent = await (findPreviousOperation[BsonEventRequestUrlValueSet](Selectors.byId(evt.id)))

      val operation = BsonEventRequestUrlBrowserSet(
        evt.requestId,
        evt.id,
        maybeBlockUrlSetEvent.map(_.to),
        maybeBlockUrlSetEvent.flatMap(_.urlType),
        maybePreviousEvent.flatMap(_.to),
        evt.browser,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestNoteUpdated(evt: EventRequestNoteUpdatedV2): Future[Unit] = async {
      val maybePreviousOperation = await (findPreviousOperation[BsonEventRequestNoteUpdated](Selectors.byRequestIdAndSource(evt.requestId, evt.source)))

      val operation = BsonEventRequestNoteUpdated(evt.requestId,
        Some(evt.source),
        maybePreviousOperation.map(_.newNote),
        evt.note,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestSubtypeUpdated(evt: EventRequestSubtypeUpdated): Future[Unit] = async {
      val maybePreviousOperation = await (findPreviousOperation[BsonEventRequestSubtypeUpdated](Selectors.byRequestId(evt.requestId)))

      val operation = BsonEventRequestSubtypeUpdated(evt.requestId,
        blockId = None,
        maybePreviousOperation.map(_.newSubtype),
        evt.subtype,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestPatternAdded(evt: EventRequestPatternAdded): Future[Unit] = async {
      val blockInfo = await(getBlockInfo(evt.blockId))

      val operation = BsonEventRequestPatternAdded(evt.requestId,
        evt.id,
        blockInfo.source,
        evt.blockId,
        blockInfo.blockType,
        blockInfo.blockTitle,
        evt.pattern,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestPatternUpdated(evt: EventRequestPatternUpdated): Future[Unit] = async {
      val patternInfo = await (getPatternInfo(evt.id))

      val operation = BsonEventRequestPatternUpdated(
        evt.requestId,
        evt.id,
        patternInfo.source,
        patternInfo.blockId,
        patternInfo.blockType,
        patternInfo.blockTitle,
        patternInfo.pattern,
        evt.pattern,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestPatternRemoved(evt: EventRequestPatternRemoved): Future[Unit] = async {
      val patternInfo = await (getPatternInfo(evt.id))

      val operation = BsonEventRequestPatternRemoved(evt.requestId,
        evt.id,
        patternInfo.source,
        patternInfo.blockType,
        patternInfo.blockTitle,
        patternInfo.pattern,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestRepositoryEventBound(evt: EventRequestAndRepositoryEventBoundV3): Future[Unit] = {

      val operation = BsonEventRequestRepositoryEventBound(evt.requestId,
        Some(evt.source),
        evt.kind,
        evt.repositoryEventId,
        evt.doneBy,
        evt.doneOn.toInstant)

      eventRequestHistoryCollection flatMap (_.insertOne(operation))
    }

    private def handleEventRequestRepositoryPatternBound(evt: EventRequestAndRepositoryPatternBound): Future[Unit] = async {
      val patternInfo = await(getPatternInfo(evt.patternId))

      val operation = BsonEventRequestRepositoryPatternBound(evt.requestId,
        patternInfo.source,
        patternInfo.blockId,
        patternInfo.blockType,
        patternInfo.blockTitle,
        evt.patternId,
        patternInfo.pattern,
        evt.repositoryPatternId,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def handleEventRequestPatternMoved(evt: EventRequestPatternMoved): Future[Unit] = async {
      val patternInfo = await (getPatternInfo(evt.id))

      val toBlockInfo = await(getBlockInfo(evt.targetBlockId))

      val operation = BsonEventRequestPatternMoved(evt.requestId,
        evt.id,
        patternInfo.source,
        patternInfo.pattern,
        patternInfo.blockId,
        patternInfo.blockType,
        patternInfo.blockTitle,
        patternInfo.source,
        evt.targetBlockId,
        toBlockInfo.blockType,
        toBlockInfo.blockTitle,
        toBlockInfo.source,
        evt.doneBy,
        evt.doneOn.toInstant)

      await (eventRequestHistoryCollection flatMap (_.insertOne(operation)))
    }

    private def eventRequestHistoryCollection: Future[MongoCollection[BsonOperation]] =
      Future.successful {
        scalaMongoDb
          .getCollection[BsonOperation](HistoryCollectionName)
          .withCodecRegistry(codecRegistry)
      }

    private def findPreviousOperation[T <: BsonOperation : ClassTag](idSelector: Bson): Future[Option[T]] = async {
      val clazz = implicitly[ClassTag[T]].runtimeClass
      val operationTag = operationTags(clazz)
      val selector = and(
        idSelector,
        Selectors.byOperationTag(operationTag)
      )

      val maybeOp = await(eventRequestHistoryCollection flatMap(_.find(selector).sort(Sorting.byDoneOn).first().toFutureOption()))

      maybeOp map {
        case op if op.getClass == clazz => op.asInstanceOf[T]
        case other => sys.error(s"Expecting operation of $clazz but got $other")
      }
    }

    private def getBlockInfo(blockId: UUID): Future[BlockInfo] = async {

      val maybeBlockAddEvent = await (findPreviousOperation[BsonEventRequestBlockAdded](Selectors.byBlockId(blockId)))
      val maybeBlockTransferEvent = await (findPreviousOperation[BsonEventRequestBlockTransferred](Selectors.byBlockId(blockId)))
      val maybeBlockTitleSetEvent = await (findPreviousOperation[BsonEventRequestBlockTitleSet](Selectors.byBlockId(blockId)))

      BlockInfo(
        Some(blockId),
        maybeBlockTransferEvent.map(_.to).orElse(maybeBlockAddEvent.flatMap(_.source)),
        maybeBlockTransferEvent.map(_.blockType).orElse(maybeBlockAddEvent.map(_.blockType)),
        maybeBlockTitleSetEvent.map(_.to).orElse(maybeBlockTransferEvent.flatMap(_.blockTitle)),
      )
    }

    private def getUrlInfo(urlId: UUID): Future[UrlInfo] = async {

      val maybeUrlUpdateEvent = await (findPreviousOperation[BsonEventRequestUrlValueSet](Selectors.byUrlId(urlId)))
      val maybeUrlMoveEvent = await (findPreviousOperation[BsonEventRequestUrlMoved](Selectors.byUrlId(urlId)))
      val maybeUrlAddEvent = await (findPreviousOperation[BsonEventRequestUrlAdded](Selectors.byUrlId(urlId)))

      val maybeOriginalUrlId: Option[UUID] = (maybeUrlUpdateEvent, maybeUrlMoveEvent, maybeUrlAddEvent) match {
        case (None, None, None) => await (findPreviousOperation[BsonEventRequestBlockTransferred](Selectors.byUrlIdIn(urlId)))
            .flatMap(_.urlsTransferred.find(_.id == urlId))
            .map(_.originalId)
        case _ => None
      }

      val maybeOriginalUrlInfo: Option[UrlInfo] = maybeOriginalUrlId match {
        case Some(originalUrlId) => Some(await (getUrlInfo(originalUrlId)))
        case _ => None
      }

      val maybeBlockId = maybeUrlUpdateEvent.flatMap(_.blockId)
        .orElse(maybeUrlMoveEvent.map(_.to))
        .orElse(maybeUrlAddEvent.map(_.blockId))
        .orElse(maybeOriginalUrlInfo.flatMap(_.blockId))

      val maybeBlockInfo: Option[BlockInfo] = maybeBlockId match {
        case Some(blockId) => Some(await (getBlockInfo(blockId)))
        case _ => None
      }

      UrlInfo(
        Some(urlId),
        maybeUrlUpdateEvent.map(_.to)
          .orElse(maybeUrlMoveEvent.flatMap(_.url))
          .orElse(maybeUrlAddEvent.map(_.url))
          .orElse(maybeOriginalUrlInfo.flatMap(_.url))
          .filter(_.nonEmpty),
        maybeUrlUpdateEvent.flatMap(_.urlType)
          .orElse(maybeUrlMoveEvent.flatMap(_.urlType))
          .orElse(maybeUrlAddEvent.map(_.urlType))
          .orElse(maybeOriginalUrlInfo.flatMap(_.urlType)),
        maybeUrlUpdateEvent.flatMap(_.browser)
          .orElse(maybeUrlMoveEvent.flatMap(_.browser))
          .orElse(maybeUrlAddEvent.flatMap(_.browser))
          .orElse(maybeOriginalUrlInfo.flatMap(_.browser)),
        maybeBlockId,
        maybeBlockInfo.flatMap(_.blockType),
        maybeBlockInfo.flatMap(_.blockTitle),
        maybeBlockInfo.flatMap(_.source),
      )
    }

    private def getPatternInfo(patternId: UUID): Future[PatternInfo] = async {

      val maybePatternUpdateEvent = await (findPreviousOperation[BsonEventRequestPatternUpdated](Selectors.byPatternId(patternId)))
      val maybePatternMoveEvent = await (findPreviousOperation[BsonEventRequestPatternMoved](Selectors.byPatternId(patternId)))
      val maybePatternAddEvent = await (findPreviousOperation[BsonEventRequestPatternAdded](Selectors.byPatternId(patternId)))

      val maybeOriginalPatternId: Option[UUID] = (maybePatternUpdateEvent, maybePatternMoveEvent, maybePatternAddEvent) match {
        case (None, None, None) => await (findPreviousOperation[BsonEventRequestBlockTransferred](Selectors.byPatternIdIn(patternId)))
            .flatMap(_.patternsTransferred.find(_.id == patternId))
            .map(_.originalId)
        case _ => None
      }

      val maybeOriginalPatternInfo: Option[PatternInfo] = maybeOriginalPatternId match {
        case Some(originalPatternId) => Some(await (getPatternInfo(originalPatternId)))
        case _ => None
      }

      val maybeBlockId = maybePatternUpdateEvent.flatMap(_.blockId)
        .orElse(maybePatternMoveEvent.map(_.to))
        .orElse(maybePatternAddEvent.map(_.blockId))
        .orElse(maybeOriginalPatternInfo.flatMap(_.blockId))

      val maybeBlockInfo: Option[BlockInfo] = maybeBlockId match {
        case Some(blockId) => Some(await (getBlockInfo(blockId)))
        case _ => None
      }

      PatternInfo(
        Some(patternId),
        maybePatternUpdateEvent.map(_.to)
          .orElse(maybePatternMoveEvent.flatMap(_.pattern))
          .orElse(maybePatternAddEvent.map(_.pattern))
          .orElse(maybeOriginalPatternInfo.flatMap(_.pattern))
          .filter(_.nonEmpty),
        maybeBlockId,
        maybeBlockInfo.flatMap(_.blockType),
        maybeBlockInfo.flatMap(_.blockTitle),
        maybeBlockInfo.flatMap(_.source),
      )
    }

    private def historySelector(requestId: UUID, maybeSource: Option[PatternDataSource], maybeEventType: Option[String]): Bson = (maybeSource, maybeEventType) match {
      case (Some(source), Some(eventType)) => Selectors.byRequestIdSourceAndEventType(requestId, source, typesToTags(eventType))
      case (Some(source), None) => Selectors.byRequestIdAndSource(requestId, source)
      case (None, Some(eventType)) => Selectors.byRequestIdAndEventType(requestId, typesToTags(eventType))
      case _ => Selectors.byRequestId(requestId)
    }

    override def clear(): Future[Unit] = eventRequestHistoryCollection flatMap (_.drop())

    override def history(eventRequestId: UUID, start: Int, limit: Int, source: Option[PatternDataSource], eventType: Option[String]): Future[PaginatedList[EventRequestHistoryReadModel.EventHistoryItem]] = async {
      val selector = historySelector(eventRequestId, source, eventType)

      val historyItems = await(eventRequestHistoryCollection flatMap (_.find(selector).sort(Sorting.byDoneOn).skip(start).limit(limit).toFuture()))
      val count = await(eventRequestHistoryCollection flatMap (_.countDocuments(selector).toFuture()))

      val jsonItems = historyItems
        .map {
        case i: BsonEventRequestAdded => EventHistoryEventRequestAdded(i.domain, i.eventType, i.eventSubtype, i.doneBy, i.doneOn)
        case i: BsonEventRequestEnabled=> EventHistoryEventEnabled(i.doneBy, i.doneOn)
        case i: BsonEventRequestDisabled => EventHistoryEventDisabled(i.message, i.doneBy, i.doneOn)
        case i: BsonEventRequestExistenceUpdated => EventHistoryEventExistenceUpdated(i.source, i.from, i.to, i.doneBy, i.doneOn)
        case i: BsonEventRequestTypeUpdated => EventHistoryEventTypeUpdated(i.source, i.from, i.to, i.doneBy, i.doneOn)
        case i: BsonEventRequestBlockAdded => EventHistoryAddedBlock(i.blockType, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestBlockRemoved => EventHistoryRemovedBlock(i.blockType, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestBlockTitleSet => EventHistoryBlockTitleSet(i.blockType, i.source, i.from, i.to, i.doneBy, i.doneOn)
        case i: BsonEventRequestBlockNoteSet => EventHistoryBlockNoteSet(i.blockId, i.blockType, i.oldNote, i.newNote, i.source, i.blockTitle, i.doneBy, i.doneOn)
        case i: BsonEventRequestBlockAnalystNoteSet => EventHistoryBlockAnalystNoteSet(i.blockId, i.blockType, i.blockTitle, i.from, i.to, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestBlockValidatorNoteSet => EventHistoryBlockValidatorNoteSet(i.blockId, i.blockType, i.blockTitle, i.from, i.to, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestBlockTransferred => EventHistoryBlockTransferred(i.blockId, i.blockType, i.blockTitle, i.from, i.to, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestBlockLabelsSet => EventHistoryBlockLabelsSet(i.blockId, i.blockType, i.blockTitle, i.from, i.to, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestBlockRangeSet => EventHistoryBlockRangeSet(i.blockId, i.blockType, i.blockTitle, i.fromFrom, i.fromTo, i.toFrom, i.toTo, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestGlobalUrlAdded => EventHistoryGlobalUrlAdded(i.urlId, i.urlType, i.browser, i.url, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestGlobalUrlValueSet => EventHistoryGlobalUrlValueSet(i.urlType, i.oldUrl, i.newUrl, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestUrlAdded => EventHistoryUrlAdded(i.urlId, i.blockId, i.blockType, i.blockTitle, i.urlType, i.browser, i.url, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestUrlRemoved => EventHistoryUrlRemoved(i.urlId, i.blockId, i.blockType, i.blockTitle, i.urlType, i.browser, i.url, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestUrlValueSet => EventHistoryUrlValueSet(i.blockType, i.blockTitle, i.urlType, i.from, i.to, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestUrlNoteSet => EventHistoryUrlNoteSet(i.blockType, i.blockTitle, i.urlType, i.from, i.to, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestUrlMoved => EventHistoryUrlMoved(i.urlType, i.url, i.from, i.fromBlockType, i.fromBlockTitle, i.fromSource, i.to, i.toBlockType, i.toBlockTitle, i.toSource, i.doneBy, i.doneOn)
        case i: BsonEventRequestUrlBrowserSet => EventHistoryUrlBrowserSet(i.urlType, i.url, i.from, i.to, i.doneBy, i.doneOn)
        case i: BsonEventRequestNoteUpdated => EventHistoryNoteUpdated(i.oldNote, i.newNote, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestSubtypeUpdated => EventHistorySubtypeUpdated(i.oldSubtype, i.newSubtype, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestPatternAdded => EventHistoryPatternAdded(i.pattern, i.source, i.blockType, i.blockTitle, i.doneBy, i.doneOn)
        case i: BsonEventRequestPatternUpdated => EventHistoryPatternUpdated(i.blockId, i.blockType, i.blockTitle, i.from, i.to, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestPatternRemoved => EventHistoryPatternRemoved(i.pattern, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestRepositoryEventBound => EventHistoryRequestRepositoryEventBound(i.kind, i.repositoryEventId, i.source, i.doneBy, i.doneOn)
        case i: BsonEventRequestRepositoryPatternBound => EventHistoryRequestRepositoryPatternBound(i.source, i.blockType, i.blockTitle, i.patternId, i.pattern, i.repositoryPatternId, i.doneBy, i.doneOn)
        case i: BsonEventRequestPatternMoved => EventHistoryRequestPatternMoved(i.patternId, i.pattern, i.from, i.fromBlockType, i.fromBlockTitle, i.fromSource, i.to, i.toBlockType, i.toBlockTitle, i.toSource, i.doneBy, i.doneOn)
      }

      PaginatedList(jsonItems, limit, start, count.toInt)
    }
  }

  object MongoEventRequestHistoryReadModel {

    sealed trait BsonOperation {
      def requestId: UUID
      def doneBy: UUID
      def doneOn: Instant
      def source: Option[PatternDataSource]
    }

    final case class BlockInfo(
      blockId: Option[UUID],
      source: Option[PatternDataSource],
      blockType: Option[BlockType],
      blockTitle: Option[String],
    )

    final case class UrlInfo(
      urlId: Option[UUID],
      url: Option[String],
      urlType: Option[UrlType],
      browser: Option[Browser],
      blockId: Option[UUID],
      blockType: Option[BlockType],
      blockTitle: Option[String],
      source: Option[PatternDataSource],
    )

    final case class PatternInfo(
      patternId: Option[UUID],
      pattern: Option[String],
      blockId: Option[UUID],
      blockType: Option[BlockType],
      blockTitle: Option[String],
      source: Option[PatternDataSource],
    )

    final case class BsonEventRequestAdded(
      requestId: UUID,
      domain: String,
      doneBy: UUID,
      doneOn: Instant,
      eventType: UUID,
      eventSubtype: UUID) extends BsonOperation {
      override val source: Option[PatternDataSource] = None
    }

    final case class BsonEventRequestEnabled(
      requestId: UUID,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation {
      override val source: Option[PatternDataSource] = None
    }

    final case class BsonEventRequestDisabled(
      requestId: UUID,
      message: Option[String],
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation {
      override val source: Option[PatternDataSource] = None
    }

    final case class BsonEventRequestTypeUpdated(
      requestId: UUID,
      source: Option[PatternDataSource],
      from: Option[UUID],
      to: UUID,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestBlockAdded(
      requestId: UUID,
      blockId: UUID,
      source: Option[PatternDataSource],
      blockType: BlockType,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestNoteUpdated(
      requestId: UUID,
      source: Option[PatternDataSource],
      oldNote: Option[String],
      newNote: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestExistenceUpdated(
      requestId: UUID,
      source: Option[PatternDataSource],
      from: Option[EventRequestExistence],
      to: EventRequestExistence,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestBlockRemoved(
      requestId: UUID,
      source: Option[PatternDataSource],
      blockType: Option[BlockType],
      blockTitle: Option[String],
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestBlockNoteSet(
      requestId: UUID,
      blockId: Option[UUID],
      blockType: Option[BlockType],
      source: Option[PatternDataSource],
      blockTitle: Option[String],
      oldNote: Option[String],
      newNote: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestBlockAnalystNoteSet(
      requestId: UUID,
      source: Option[PatternDataSource],
      blockId: Option[UUID],
      blockType: Option[BlockType],
      blockTitle: Option[String],
      from: Option[String],
      to: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestBlockValidatorNoteSet(
      requestId: UUID,
      blockId: Option[UUID],
      source: Option[PatternDataSource],
      blockType: Option[BlockType],
      blockTitle: Option[String],
      from: Option[String],
      to: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestBlockTransferred(
      requestId: UUID,
      blockId: UUID,
      source: Option[PatternDataSource],
      blockType: BlockType,
      blockTitle: Option[String],
      urls: Seq[UUID],
      urlsTransferred: Seq[UrlTransfer],
      patterns: Seq[UUID],
      patternsTransferred: Seq[PatternTransfer],
      from: Option[PatternDataSource],
      to: PatternDataSource,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestBlockTitleSet(
      requestId: UUID,
      blockId: UUID,
      source: Option[PatternDataSource],
      blockType: Option[BlockType],
      from: Option[String],
      to: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestBlockLabelsSet(
      requestId: UUID,
      source: Option[PatternDataSource],
      blockId: UUID,
      blockType: Option[BlockType],
      blockTitle: Option[String],
      from: Option[Set[String]],
      to: Set[String],
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestBlockRangeSet(
      requestId: UUID,
      source: Option[PatternDataSource],
      blockId: UUID,
      blockType: Option[BlockType],
      blockTitle: Option[String],
      fromFrom: Option[LocalDate],
      fromTo: Option[LocalDate],
      toFrom: Option[LocalDate],
      toTo: Option[LocalDate],
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestGlobalUrlAdded(
      requestId: UUID,
      urlId: UUID,
      source: Option[PatternDataSource],
      urlType: UrlType,
      browser: Option[Browser],
      url: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation {
    }

    final case class BsonEventRequestGlobalUrlValueSet(
      requestId: UUID,
      urlId: UUID,
      source: Option[PatternDataSource],
      browser: Option[Browser],
      urlType: Option[UrlType],
      oldUrl: Option[String],
      newUrl: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestUrlAdded(
      requestId: UUID,
      urlId: UUID,
      source: Option[PatternDataSource],
      blockId: UUID,
      blockType: Option[BlockType],
      blockTitle: Option[String],
      urlType: UrlType,
      browser: Option[Browser],
      url: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestUrlRemoved(
      requestId: UUID,
      urlId: UUID,
      source: Option[PatternDataSource],
      blockId: Option[UUID],
      blockType: Option[BlockType],
      blockTitle: Option[String],
      urlType: Option[UrlType],
      browser: Option[Browser],
      url: Option[String],
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestUrlNoteSet(
      requestId: UUID,
      urlId: UUID,
      source: Option[PatternDataSource],
      blockId: Option[UUID],
      blockType: Option[BlockType],
      blockTitle: Option[String],
      urlType: Option[UrlType],
      browser: Option[Browser],
      from: Option[String],
      to: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestUrlMoved(
      requestId: UUID,
      urlId: UUID,
      url: Option[String],
      urlType: Option[UrlType],
      browser: Option[Browser],
      from: Option[UUID],
      fromBlockType: Option[BlockType],
      fromBlockTitle: Option[String],
      fromSource: Option[PatternDataSource],
      to: UUID,
      toBlockType: Option[BlockType],
      toBlockTitle: Option[String],
      toSource: Option[PatternDataSource],
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation {
      override val source: Option[PatternDataSource] = None
    }

    final case class BsonEventRequestUrlBrowserSet(
      requestId: UUID,
      urlId: UUID,
      url: Option[String],
      urlType: Option[UrlType],
      from: Option[Browser],
      to: Option[Browser],
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation {
      override val source: Option[PatternDataSource] = None
    }

    final case class BsonEventRequestUrlValueSet(
      requestId: UUID,
      urlId: UUID,
      source: Option[PatternDataSource],
      blockId: Option[UUID],
      blockType: Option[BlockType],
      blockTitle: Option[String],
      browser: Option[Browser],
      urlType: Option[UrlType],
      from: Option[String],
      to: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestSubtypeUpdated(
      requestId: UUID,
      blockId: Option[UUID],
      oldSubtype: Option[UUID],
      newSubtype: UUID,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation {
      override val source: Option[PatternDataSource] = None
    }

    final case class BsonEventRequestPatternAdded(
      requestId: UUID,
      patternId: UUID,
      source: Option[PatternDataSource],
      blockId: UUID,
      blockType: Option[BlockType],
      blockTitle: Option[String],
      pattern: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestPatternUpdated(
      requestId: UUID,
      patternId: UUID,
      source: Option[PatternDataSource],
      blockId: Option[UUID],
      blockType: Option[BlockType],
      blockTitle: Option[String],
      from: Option[String],
      to: String,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestPatternMoved(
      requestId: UUID,
      patternId: UUID,
      source: Option[PatternDataSource],
      pattern: Option[String],
      from: Option[UUID],
      fromBlockType: Option[BlockType],
      fromBlockTitle: Option[String],
      fromSource: Option[PatternDataSource],
      to: UUID,
      toBlockType: Option[BlockType],
      toBlockTitle: Option[String],
      toSource: Option[PatternDataSource],
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestPatternRemoved(
      requestId: UUID,
      patternIn: UUID,
      source: Option[PatternDataSource],
      blockType: Option[BlockType],
      blockTitle: Option[String],
      pattern: Option[String],
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestRepositoryEventBound(
      requestId: UUID,
      source: Option[PatternDataSource],
      kind: EventRepositoryBindingKind,
      repositoryEventId: Int,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    final case class BsonEventRequestRepositoryPatternBound(
      requestId: UUID,
      source: Option[PatternDataSource],
      blockId: Option[UUID],
      blockType: Option[BlockType],
      blockTitle: Option[String],
      patternId: UUID,
      pattern: Option[String],
      repositoryPatternId: Int,
      doneBy: UUID,
      doneOn: Instant) extends BsonOperation

    private val operationTags: Map[Class[_], String] = Map(
      classOf[BsonEventRequestAdded] -> "request-added",
      classOf[BsonEventRequestEnabled] -> "request-enabled",
      classOf[BsonEventRequestDisabled] -> "request-disabled",
      classOf[BsonEventRequestExistenceUpdated] -> "existence-updated",
      classOf[BsonEventRequestBlockAdded] -> "block-added",
      classOf[BsonEventRequestBlockRemoved] -> "block-removed",
      classOf[BsonEventRequestBlockTitleSet] -> "block-title-updated",
      classOf[BsonEventRequestBlockNoteSet] -> "block-note-updated",
      classOf[BsonEventRequestBlockAnalystNoteSet] -> "block-analyst-note-updated",
      classOf[BsonEventRequestBlockValidatorNoteSet] -> "block-validator-note-updated",
      classOf[BsonEventRequestBlockTransferred] -> "block-transferred",
      classOf[BsonEventRequestBlockLabelsSet] -> "block-labels-set",
      classOf[BsonEventRequestBlockRangeSet] -> "block-range-set",
      classOf[BsonEventRequestNoteUpdated] -> "note-updated",
      classOf[BsonEventRequestGlobalUrlAdded] -> "global-url-added",
      classOf[BsonEventRequestGlobalUrlValueSet] -> "global-url-value-set",
      classOf[BsonEventRequestUrlAdded] -> "url-added",
      classOf[BsonEventRequestUrlRemoved] -> "url-removed",
      classOf[BsonEventRequestUrlValueSet] -> "url-value-set",
      classOf[BsonEventRequestUrlNoteSet] -> "url-note-set",
      classOf[BsonEventRequestUrlMoved] -> "url-moved",
      classOf[BsonEventRequestUrlBrowserSet] -> "url-browser-set",
      classOf[BsonEventRequestTypeUpdated] -> "type-updated",
      classOf[BsonEventRequestSubtypeUpdated] -> "subtype-updated",
      classOf[BsonEventRequestPatternAdded] -> "pattern-added",
      classOf[BsonEventRequestPatternUpdated] -> "pattern-updated",
      classOf[BsonEventRequestPatternRemoved] -> "pattern-removed",
      classOf[BsonEventRequestRepositoryEventBound] -> "repository-event-bound",
      classOf[BsonEventRequestRepositoryPatternBound] -> "repository-pattern-bound",
      classOf[BsonEventRequestPatternMoved] -> "pattern-moved",
    )

    private val typesToTags: Map[String, String] = Map(
      "eventRequestAdded" -> "request-added",
      "eventRequestEnabled" -> "request-enabled",
      "eventRequestDisabled" -> "request-disabled",
      "eventRequestExistenceUpdated" -> "existence-updated",
      "eventRequestBlockAdded" -> "block-added",
      "eventRequestBlockRemoved" -> "block-removed",
      "eventRequestBlockTitleSet" -> "block-title-updated",
      "eventRequestBlockNoteSet" -> "block-note-updated",
      "eventRequestBlockAnalystNoteSet" -> "block-analyst-note-updated",
      "eventRequestBlockValidatorNoteSet" -> "block-validator-note-updated",
      "eventRequestBlockTransferred" -> "block-transferred",
      "eventRequestBlockLabelsSet" -> "block-labels-set",
      "eventRequestBlockRangeSet" -> "block-range-set",
      "eventRequestNoteUpdated" -> "note-updated",
      "eventRequestGlobalUrlAdded" -> "global-url-added",
      "eventRequestGlobalUrlValueSet" -> "global-url-value-set",
      "eventRequestUrlAdded" -> "url-added",
      "eventRequestUrlRemoved" -> "url-removed",
      "eventRequestUrlValueSet" -> "url-value-set",
      "eventRequestUrlNoteSet" -> "url-note-set",
      "eventRequestUrlMoved" -> "url-moved",
      "eventRequestUrlBrowserSet" -> "url-browser-set",
      "eventRequestTypeUpdated" -> "type-updated",
      "eventRequestSubtypeUpdated" -> "subtype-updated",
      "eventRequestPatternAdded" -> "pattern-added",
      "eventRequestPatternUpdated" -> "pattern-updated",
      "eventRequestPatternRemoved" -> "pattern-removed",
      "eventRequestAndRepositoryEventBound" -> "repository-event-bound",
      "eventRequestAndRepositoryPatternBound" -> "repository-pattern-bound",
      "eventRequestPatternMoved" -> "pattern-moved",
    )

    private val codecRegistry = fromRegistries(fromProviders(
      new PolymorphicCodecProvider[BsonOperation](Seq(
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestAdded]), Macros.createCodecIgnoreNone[BsonEventRequestAdded](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestEnabled]), Macros.createCodecIgnoreNone[BsonEventRequestEnabled](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestDisabled]), Macros.createCodecIgnoreNone[BsonEventRequestDisabled](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestExistenceUpdated]), Macros.createCodecIgnoreNone[BsonEventRequestExistenceUpdated](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestBlockAdded]), Macros.createCodecIgnoreNone[BsonEventRequestBlockAdded](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestBlockRemoved]), Macros.createCodecIgnoreNone[BsonEventRequestBlockRemoved](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestBlockTitleSet]), Macros.createCodecIgnoreNone[BsonEventRequestBlockTitleSet](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestBlockNoteSet]), Macros.createCodecIgnoreNone[BsonEventRequestBlockNoteSet](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestBlockAnalystNoteSet]), Macros.createCodecIgnoreNone[BsonEventRequestBlockAnalystNoteSet](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestBlockValidatorNoteSet]), Macros.createCodecIgnoreNone[BsonEventRequestBlockValidatorNoteSet](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestBlockTransferred]), Macros.createCodecIgnoreNone[BsonEventRequestBlockTransferred](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestBlockLabelsSet]), Macros.createCodecIgnoreNone[BsonEventRequestBlockLabelsSet](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestBlockRangeSet]), Macros.createCodecIgnoreNone[BsonEventRequestBlockRangeSet](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestNoteUpdated]), Macros.createCodecIgnoreNone[BsonEventRequestNoteUpdated](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestGlobalUrlAdded]), Macros.createCodecIgnoreNone[BsonEventRequestGlobalUrlAdded](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestGlobalUrlValueSet]), Macros.createCodecIgnoreNone[BsonEventRequestGlobalUrlValueSet](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestUrlAdded]), Macros.createCodecIgnoreNone[BsonEventRequestUrlAdded](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestUrlValueSet]), Macros.createCodecIgnoreNone[BsonEventRequestUrlValueSet](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestUrlRemoved]), Macros.createCodecIgnoreNone[BsonEventRequestUrlRemoved](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestUrlNoteSet]), Macros.createCodecIgnoreNone[BsonEventRequestUrlNoteSet](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestUrlMoved]), Macros.createCodecIgnoreNone[BsonEventRequestUrlMoved](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestUrlBrowserSet]), Macros.createCodecIgnoreNone[BsonEventRequestUrlBrowserSet](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestSubtypeUpdated]), Macros.createCodecIgnoreNone[BsonEventRequestSubtypeUpdated](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestPatternAdded]), Macros.createCodecIgnoreNone[BsonEventRequestPatternAdded](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestPatternUpdated]), Macros.createCodecIgnoreNone[BsonEventRequestPatternUpdated](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestPatternRemoved]), Macros.createCodecIgnoreNone[BsonEventRequestPatternRemoved](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestRepositoryEventBound]), Macros.createCodecIgnoreNone[BsonEventRequestRepositoryEventBound](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestRepositoryPatternBound]), Macros.createCodecIgnoreNone[BsonEventRequestRepositoryPatternBound](DefaultCodecRegistry)),
        PolymorphicCodecProvider.Definition(operationTags(classOf[BsonEventRequestPatternMoved]), Macros.createCodecIgnoreNone[BsonEventRequestPatternMoved](DefaultCodecRegistry)),
      )),
    ), DefaultCodecRegistry)

    private val HistoryCollectionName = "event-request-history"

    private object Fields {
      val _Id = "_id"
      val EventType = "tag"
      val Id = "data.id"
      val DoneOn = "data.doneOn"
      val RequestId = "data.requestId"
      val PatternId = "data.patternId"
      val UrlId = "data.urlId"
      val BlockId = "data.blockId"
      val Source = "data.source"
      val Urls = "data.urls"
      val UrlsTransferred = "data.urlsTransferred"
      val Patterns = "data.patterns"
      val PatternsTransferred = "data.patternsTransferred"
    }

    private object Selectors {

      def byRequestId(requestId: UUID): Bson =
        equal(Fields.RequestId, requestId)

      def byRequestIdAndSource(requestId: UUID, source: PatternDataSource): Bson =
        and(equal(Fields.RequestId, requestId), equal(Fields.Source, source))

      def byRequestIdAndEventType(requestId: UUID, eventType: String): Bson =
        and(equal(Fields.RequestId, requestId), equal(Fields.EventType, eventType))

      def byRequestIdSourceAndEventType(requestId: UUID, source: PatternDataSource, eventType: String): Bson =
        and(equal(Fields.RequestId, requestId), equal(Fields.Source, source), equal(Fields.EventType, eventType))

      def byId(id: UUID): Bson =
        equal(Fields.Id, id)

      def byPatternId(patternId: UUID): Bson =
        equal(Fields.PatternId, patternId)

      def byUrlId(urlId: UUID): Bson =
        equal(Fields.UrlId, urlId)

      def byUrlIdIn(urlId: UUID): Bson =
        in(Fields.Urls, urlId)

      def byPatternIdIn(patternId: UUID): Bson =
        in(Fields.Patterns, patternId)

      def byBlockId(blockId: UUID): Bson =
        equal(Fields.BlockId, blockId)

      def byOperationTag(tag: String): Bson =
        equal(Fields.EventType, tag)

    }
    object Sorting {
      val byDoneOn: Bson = descending(Fields.DoneOn, Fields._Id)
    }
  }

}
