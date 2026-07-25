import httpx
import pytest

from signalkit_stream.collectors.github import GitHubCollector
from signalkit_stream.models import SignalKind


@pytest.mark.asyncio
async def test_github_issue_and_comment_collection() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/search/issues":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "node_id": "ISSUE_NODE",
                            "repository_url": "https://api.github.com/repos/acme/app",
                            "number": 42,
                            "title": "Need webhook support",
                            "body": "We are looking for webhook support.",
                            "html_url": "https://github.com/acme/app/issues/42",
                            "created_at": "2026-07-01T10:00:00Z",
                            "state": "open",
                            "comments": 1,
                            "labels": [{"name": "feature"}],
                            "user": {"login": "alice"},
                        }
                    ]
                },
            )
        if request.url.path == "/repos/acme/app/issues/42/comments":
            return httpx.Response(
                200,
                json=[
                    {
                        "node_id": "COMMENT_NODE",
                        "id": 9,
                        "body": "Same need here.",
                        "html_url": "https://github.com/acme/app/issues/42#issuecomment-9",
                        "created_at": "2026-07-01T11:00:00Z",
                        "user": {"login": "bob"},
                    }
                ],
            )
        raise AssertionError(f"unexpected request: {request.url}")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        events = await GitHubCollector(
            '"looking for" is:issue',
            include_comments=True,
            comments_per_item=2,
            client=client,
        ).collect(limit=1)

    assert len(events) == 2
    assert events[0].kind is SignalKind.ISSUE
    assert events[0].metadata["labels"] == ["feature"]
    assert events[1].kind is SignalKind.COMMENT
    assert events[1].metadata["parent_event_id"] == events[0].id
