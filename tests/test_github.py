import httpx
import pytest

from signalkit_stream.collectors.github import GitHubCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    Cursor,
)


def issue(number: int, *, comments: int = 0) -> dict:
    return {
        "node_id": f"ISSUE_NODE_{number}",
        "repository_url": "https://api.github.com/repos/acme/app",
        "number": number,
        "title": f"Issue {number}",
        "body": "We are looking for webhook support.",
        "html_url": f"https://github.com/acme/app/issues/{number}",
        "created_at": f"2026-07-{number:02d}T10:00:00Z",
        "updated_at": f"2026-07-{number:02d}T11:00:00Z",
        "state": "open",
        "comments": comments,
        "labels": [{"name": "feature"}],
        "user": {"login": "alice"},
    }


@pytest.mark.asyncio
async def test_github_issue_comment_and_partial_page_cursor() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/search/issues":
            return httpx.Response(
                200,
                json={"total_count": 3, "items": [issue(1, comments=1), issue(2), issue(3)]},
                request=request,
            )
        if request.url.path == "/repos/acme/app/issues/1/comments":
            return httpx.Response(
                200,
                json=[
                    {
                        "node_id": "COMMENT_NODE",
                        "id": 9,
                        "body": "Same need here.",
                        "html_url": "https://github.com/acme/app/issues/1#issuecomment-9",
                        "created_at": "2026-07-01T12:00:00Z",
                        "updated_at": "2026-07-01T12:30:00Z",
                        "user": {"login": "bob"},
                    }
                ],
                request=request,
            )
        raise AssertionError(f"unexpected request: {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GitHubCollector(
            '"looking for" is:issue',
            include_comments=True,
            comments_per_item=2,
            client=client,
        )
        first = await collector.collect(context=CollectorContext(limit=2))

    assert first.events[0].kind is SignalKind.ISSUE
    assert first.events[0].metadata["labels"] == ["feature"]
    assert first.events[1].kind is SignalKind.COMMENT
    assert first.primary_count == 2
    assert first.has_more is True


@pytest.mark.asyncio
async def test_github_cursor_can_resume_inside_same_page() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"total_count": 3, "items": [issue(1), issue(2), issue(3)]},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GitHubCollector("is:issue", client=client)
        first = await collector.collect(context=CollectorContext(limit=2))
        second = await collector.collect(context=CollectorContext(limit=1), cursor=first.cursor)

    assert [event.metadata["number"] for event in first.events] == [1, 2]
    assert [event.metadata["number"] for event in second.events] == [3]


@pytest.mark.asyncio
async def test_github_html_error_page_is_classified_as_parse_error() -> None:
    """A 200 that serves HTML must not leak a raw JSONDecodeError."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"<html><body>unicorn</body></html>",
            headers={"Content-Type": "text/html"},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GitHubCollector("is:issue", client=client)
        with pytest.raises(CollectorError) as caught:
            await collector.collect(context=CollectorContext(limit=2))

    assert caught.value.kind is CollectorErrorKind.PARSE
    assert caught.value.retryable is False
    assert "JSON" in str(caught.value)


@pytest.mark.asyncio
async def test_github_search_payload_must_be_an_object_with_item_list() -> None:
    for payload in ([1, 2, 3], {"items": {"nope": True}}, {"items": ["not-an-object"]}):
        def handler(request: httpx.Request, body: object = payload) -> httpx.Response:
            return httpx.Response(200, json=body, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            collector = GitHubCollector("is:issue", client=client)
            with pytest.raises(CollectorError) as caught:
                await collector.collect(context=CollectorContext(limit=2))

        assert caught.value.kind is CollectorErrorKind.PARSE, payload


@pytest.mark.asyncio
async def test_github_malformed_timestamp_is_classified_as_parse_error() -> None:
    broken = issue(1)
    broken["created_at"] = "not-a-timestamp"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"total_count": 1, "items": [broken]}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GitHubCollector("is:issue", client=client)
        with pytest.raises(CollectorError) as caught:
            await collector.collect(context=CollectorContext(limit=2))

    assert caught.value.kind is CollectorErrorKind.PARSE
    assert "not-a-timestamp" in str(caught.value.details)


@pytest.mark.asyncio
async def test_github_comment_html_error_page_is_classified_as_parse_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/search/issues":
            return httpx.Response(
                200,
                json={"total_count": 1, "items": [issue(1, comments=1)]},
                request=request,
            )
        return httpx.Response(200, content=b"<html>maintenance</html>", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GitHubCollector(
            "is:issue",
            include_comments=True,
            comments_per_item=2,
            client=client,
        )
        with pytest.raises(CollectorError) as caught:
            await collector.collect(context=CollectorContext(limit=2))

    assert caught.value.kind is CollectorErrorKind.PARSE


@pytest.mark.asyncio
async def test_github_corrupt_cursor_watermark_is_classified_as_cursor_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
        return httpx.Response(200, json={"total_count": 0, "items": []}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GitHubCollector("is:issue", client=client)
        cursor = Cursor(collector.identity.key, {"watermark": "yesterday-ish"})
        with pytest.raises(CollectorError) as caught:
            await collector.collect(context=CollectorContext(limit=2), cursor=cursor)

    assert caught.value.kind is CollectorErrorKind.CURSOR
