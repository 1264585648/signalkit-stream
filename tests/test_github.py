import httpx
import pytest

from signalkit_stream.collectors.github import GitHubCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import CollectorContext


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


def comment(comment_id: int) -> dict:
    return {
        "node_id": f"COMMENT_NODE_{comment_id}",
        "id": comment_id,
        "body": f"comment {comment_id}",
        "html_url": f"https://github.com/acme/app/issues/1#issuecomment-{comment_id}",
        "created_at": f"2026-07-01T12:{comment_id:02d}:00Z",
        "updated_at": f"2026-07-01T12:{comment_id:02d}:30Z",
        "user": {"login": "bob"},
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
            assert request.url.params["direction"] == "desc"
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
async def test_github_uses_inclusive_watermark_overlap() -> None:
    queries: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        queries.append(request.url.params["q"])
        return httpx.Response(
            200,
            json={"total_count": 1, "items": [issue(1)]},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GitHubCollector("is:issue", client=client)
        first = await collector.collect(context=CollectorContext(limit=10))
        await collector.collect(context=CollectorContext(limit=10), cursor=first.cursor)

    assert "updated:>=2026-07-01T11:00:00Z" in queries[1]


@pytest.mark.asyncio
async def test_github_collects_latest_comment_window_chronologically() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/search/issues":
            return httpx.Response(
                200,
                json={
                    "total_count": 1,
                    "items": [issue(1, comments=20)],
                    "incomplete_results": True,
                },
                request=request,
            )
        if request.url.path == "/repos/acme/app/issues/1/comments":
            assert request.url.params["per_page"] == "2"
            assert request.url.params["sort"] == "created"
            assert request.url.params["direction"] == "desc"
            return httpx.Response(
                200,
                json=[comment(3), comment(2)],
                request=request,
            )
        raise AssertionError(f"unexpected request: {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await GitHubCollector(
            "is:issue",
            include_comments=True,
            comments_per_item=2,
            client=client,
        ).collect()

    comments = [event for event in result.events if event.kind is SignalKind.COMMENT]
    assert [event.metadata["external_id"] for event in comments] == [
        "COMMENT_NODE_2",
        "COMMENT_NODE_3",
    ]
    assert any("incomplete_results" in warning for warning in result.warnings)
