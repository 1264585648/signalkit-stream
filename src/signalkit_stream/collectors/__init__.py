from signalkit_stream.collectors.base import Collector
from signalkit_stream.collectors.github import GitHubCollector
from signalkit_stream.collectors.hackernews import HackerNewsCollector
from signalkit_stream.collectors.rss import RSSCollector

__all__ = ["Collector", "GitHubCollector", "HackerNewsCollector", "RSSCollector"]
