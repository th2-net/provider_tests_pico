from datetime import datetime, timezone
from typing import List

# from th2_data_services.data_source.lwdp.filters.filter import Filter
# from th2_data_services.provider.v6.interfaces.command import IHTTPProvider6Command
# from th2_data_services.provider.v6.data_source import HTTPProvider6DataSource
# from th2_data_services.provider.v6.provider_api import HTTPProvider6API


class GetEventsUrl:
    def __init__(
            self,
            start_timestamp: datetime,
            book_id: str,
            scopes: List[str],
            end_timestamp: datetime = None,
            parent_event: str = None,
            search_direction: str = "next",
            #resume_from_id: str = None,
            result_count_limit: int = None,
            keep_open: bool = False,
            limit_for_parent: int = None,
            metadata_only: bool = False,
            attached_messages: bool = False,
            # filters: (Filter, List[Filter]) = None,
    ):
        """
        Args:
            start_timestamp: Start timestamp of search.
            end_timestamp: End timestamp of search.
            parent_event: Match events to the specified parent.
            search_direction: Search direction.
        ###    resume_from_id: Event id from which search starts.
            result_count_limit: Result count limit.
            keep_open: If the search has reached the current moment.
                It is necessary to wait further for the appearance of new data.
                the one closest to the specified timestamp.
            limit_for_parent: How many children events for each parent do we want to request.
            metadata_only: Receive only metadata (true) or entire event (false) (without attached_messages).
            attached_messages: Gets messages ids which linked to events.
            filters: Filters using in search for messages.

        """
        self._start_timestamp = int(1000 * start_timestamp.replace(tzinfo=timezone.utc).timestamp())
        self._book_id = book_id
        self._scopes = scopes
        self._end_timestamp = int(1000 * end_timestamp.replace(tzinfo=timezone.utc).timestamp())
        self._parent_event = parent_event
        self._search_direction = search_direction
        #self._resume_from_id = resume_from_id
        self._result_count_limit = result_count_limit
        self._keep_open = keep_open
        self._limit_for_parent = limit_for_parent
        self._metadata_only = metadata_only
        self._attached_messages = attached_messages
        # self._filters = filters
        # if isinstance(filters, Filter):
        #     self._filters = filters.url()
        # elif isinstance(filters, (tuple, list)):
        #     self._filters = "".join([filter_.url() for filter_ in filters])

    # Typing temporarily removed to make RDP5 and RDP6 compatibility
    # def handle(self, data_source: HTTPProvider6DataSource):
    def handle(self, data_source):
        api = data_source.source_api
        url = api.get_url_search_sse_events(
            start_timestamp=self._start_timestamp,
            book_id=self._book_id,
            scope=self._scopes[0],  # TODO: multi scope
            end_timestamp=self._end_timestamp,
            parent_event=self._parent_event,
            search_direction=self._search_direction,
            #resume_from_id=self._resume_from_id,
            result_count_limit=self._result_count_limit,
            #keep_open=self._keep_open,
            #limit_for_parent=self._limit_for_parent,
            #metadata_only=self._metadata_only,
            #attached_messages=self._attached_messages,
            filters=self._filters,
        )

        return url


class GetMessagesByGroupsUrl:
    def __init__(
            self,
            start_timestamp: datetime,
            book_id: str,
            streams: List[str],
            groups: List[str],
            end_timestamp: datetime = None,
            keep_open: bool = False,
            message_id: List[str] = None,
    ):
        """
        Args:
            start_timestamp: Start timestamp of search.
            end_timestamp: End timestamp of search.
            streams: Alias of messages.
            resume_from_id: Message id from which search starts.
            search_direction: Search direction.
            result_count_limit: Result count limit.
            keep_open: If the search has reached the current moment.
                It is necessary to wait further for the appearance of new data.
            message_id: List of message IDs to restore search. If given, it has
                the highest priority and ignores stream (uses streams from ids), startTimestamp and resumeFromId.
            attached_events: If true, additionally load attached_event_ids
            lookup_limit_days: The number of days that will be viewed on
                the first request to get the one closest to the specified timestamp.
            filters: Filters using in search for messages.
        """
        super().__init__()
        self._start_timestamp = int(1000000 * start_timestamp.replace(tzinfo=timezone.utc).timestamp())
        self._book_id = book_id
        self._end_timestamp = (
            end_timestamp
            if end_timestamp is None
            else int(1000000 * end_timestamp.replace(tzinfo=timezone.utc).timestamp())
        )
        self._streams = streams
        self._groups = groups
        self._keep_open = keep_open
        self._message_id = message_id

    # Typing temporarily removed to make RDP5 and RDP6 compatibility
    # def handle(self, data_source: HTTPProvider6DataSource) -> str:
    def handle(self, data_source) -> str:
        api = data_source.source_api
        url = api.get_url_search_messages_by_groups(
            start_timestamp=self._start_timestamp,
            end_timestamp=self._end_timestamp,
            book_id=self._book_id,
            groups=self._groups,
            streams=self._streams,
            keep_open=self._keep_open,
        )
        return url
