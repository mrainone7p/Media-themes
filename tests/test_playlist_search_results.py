import sys
import types
from unittest import TestCase
from unittest.mock import patch

sys.modules.setdefault("requests", types.SimpleNamespace())
sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *_args, **_kwargs: {}, safe_dump=lambda *_args, **_kwargs: ""))

from web import integrations, services


class PlaylistSearchResultTests(TestCase):
    def test_playlist_search_expands_first_playlist_into_tracks(self):
        playlists = [
            {"title": "Movie OST Playlist", "url": "https://youtube.com/playlist?list=abc", "duration": ""},
            {"title": "Another Playlist", "url": "https://youtube.com/playlist?list=def", "duration": ""},
        ]
        tracks = [
            {"playlist_index": "1", "title": "Opening Theme", "url": "https://youtube.com/watch?v=track1", "duration": "4:03"},
            {"playlist_index": "2", "title": "Second Theme", "url": "https://youtube.com/watch?v=track2", "duration": "3:40"},
        ]
        with patch.object(integrations, "youtube_search", return_value=playlists) as search_mock, patch.object(
            integrations, "youtube_playlist_entries", return_value=tracks
        ) as entries_mock:
            result = integrations.youtube_playlist_search("12 Angry Men soundtrack playlist", None)

        search_mock.assert_called_once_with("12 Angry Men soundtrack playlist", None)
        entries_mock.assert_called_once_with("https://youtube.com/playlist?list=abc", None)
        self.assertEqual("Opening Theme", result[0]["title"])
        self.assertEqual("https://youtube.com/playlist?list=abc", result[0]["playlist_url"])
        self.assertEqual("Movie OST Playlist", result[0]["playlist_title"])
        self.assertEqual("Second Theme", result[1]["title"])

    def test_youtube_search_payload_uses_playlist_expansion_for_playlist_method(self):
        with patch.object(services, "load_config", return_value={"cookies_file": ""}), patch.object(
            integrations, "youtube_playlist_search", return_value=[{"title": "Opening Theme", "url": "https://youtube.com/watch?v=track1"}]
        ) as playlist_mock, patch.object(integrations, "youtube_search") as search_mock:
            payload = services.youtube_search_payload({"query": "12 Angry Men soundtrack playlist", "method": "playlist"})

        self.assertTrue(payload["ok"])
        self.assertEqual("Opening Theme", payload["results"][0]["title"])
        playlist_mock.assert_called_once_with("12 Angry Men soundtrack playlist", None)
        search_mock.assert_not_called()
