#!/usr/bin/env python3
"""
YouTube API Diagnostic Tool

Tests all YouTube-related APIs used by the Pleiades integration tests:
1. YouTube Data API v3 (search)
2. YouTube Transcript API (with cookies)
3. yt-dlp download (with cookies)
4. HuggingFace Vault operations

Usage:
    python tools/test_youtube_apis.py

Requires .env file with:
    - YOUTUBE_API_KEY_POOL_JSON
    - YOUTUBE_COOKIES_PATH
    - HF_TOKEN
    - HF_DATASET_ID
"""

import json
import sys
from datetime import datetime
from pathlib import Path

# Add project src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "atlas" / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "maia" / "src"))


def load_env():
    """Load environment from .env file."""
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        print("❌ .env file not found!")
        return {}
    
    env = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env[key] = value
    return env


def test_youtube_data_api(env: dict) -> bool:
    """Test YouTube Data API v3 with the key pool."""
    print("\n" + "="*60)
    print("1. Testing YouTube Data API v3")
    print("="*60)
    
    api_key_pool = json.loads(env.get('YOUTUBE_API_KEY_POOL_JSON', '[]'))
    if not api_key_pool:
        print("❌ No API keys found in YOUTUBE_API_KEY_POOL_JSON")
        return False
    
    print(f"📋 Found {len(api_key_pool)} API keys in pool")
    
    import urllib.parse
    import urllib.request
    
    for i, api_key in enumerate(api_key_pool):
        try:
            print(f"\n🔑 Testing key {i+1}/{len(api_key_pool)}: [redacted]")
            
            # Test search endpoint using urllib
            params = urllib.parse.urlencode({
                "part": "snippet",
                "q": "blender tutorial",
                "type": "video",
                "order": "date",
                "publishedAfter": "2023-01-01T00:00:00Z",
                "maxResults": 5,
                "key": api_key
            })
            
            url = f"https://www.googleapis.com/youtube/v3/search?{params}"
            with urllib.request.urlopen(url, timeout=10) as response:
                data = json.loads(response.read())
            
            items = data.get('items', [])
            print(f"   ✅ SUCCESS! Found {len(items)} videos")
            if items:
                print(f"   Sample: {items[0]['snippet']['title']}")
            return True
                
        except urllib.error.HTTPError as e:
            error_body = json.loads(e.read())
            error_info = error_body.get('error', {})
            reason = error_info.get('errors', [{}])[0].get('reason', 'unknown')
            if reason in ('quotaExceeded', 'dailyLimitExceeded'):
                print("   ❌ Quota exceeded for this key")
            else:
                print(f"   ❌ HTTP Error {e.code}: {reason}")
        except Exception as e:
            print(f"   ❌ Exception: {e}")
    
    print("\n❌ All API keys failed!")
    return False


def test_youtube_transcript_api(env: dict) -> bool:
    """Test YouTube Transcript API with cookies."""
    print("\n" + "="*60)
    print("2. Testing YouTube Transcript API")
    print("="*60)
    
    cookies_path = env.get('YOUTUBE_COOKIES_PATH', '')
    if not cookies_path:
        print("⚠️  YOUTUBE_COOKIES_PATH not set, skipping transcript test")
        return False
    
    cookies_file = Path(cookies_path)
    if not cookies_file.exists():
        print(f"❌ Cookies file not found: {cookies_path}")
        return False
    
    print(f"📋 Found cookies file: {cookies_path}")
    
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        from youtube_transcript_api._errors import (
            NoTranscriptFound,
            TranscriptsDisabled,
        )
        
        # Test with a known video that has captions (Blender donut tutorial by Andrew Price)
        test_video_id = "a7G3tedYKh4"  # Blender 4.0 Beginner Tutorial
        
        print(f"\n🔤 Testing transcript for video: {test_video_id}")
        
        try:
            # Try without cookies first (for auto-generated captions)
            print("   Trying auto-generated captions...")
            api = YouTubeTranscriptApi()
            transcripts = api.list(test_video_id)
            
            transcript_list = list(transcripts)
            print(f"   ✅ Found {len(transcript_list)} transcript(s)")
            
            for t in transcript_list[:3]:
                print(f"   - {t.language_code}: {t.language} ({t.is_generated and 'auto' or 'manual'})")
            
            return True
            
        except (TranscriptsDisabled, NoTranscriptFound) as e:
            print(f"   ⚠️  No transcript available without cookies: {e}")
            
            # Try with cookies if available
            print("   Trying with cookies...")
            import http.cookiejar as cookiejar

            import requests
            
            session = requests.Session()
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            })
            jar = cookiejar.MozillaCookieJar(str(cookies_file))
            jar.load(ignore_discard=True, ignore_expires=True)
            session.cookies = jar
            
            api_with_cookies = YouTubeTranscriptApi(http_client=session)
            transcripts = api_with_cookies.list(test_video_id)
            
            transcript_list = list(transcripts)
            print(f"   ✅ Found {len(transcript_list)} transcript(s) with cookies")
            return True
            
    except ImportError as e:
        print(f"❌ Module not installed: {e}")
        return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


def test_ytdlp_download(env: dict) -> bool:
    """Test yt-dlp download with cookies."""
    print("\n" + "="*60)
    print("3. Testing yt-dlp Download")
    print("="*60)
    
    cookies_path = env.get('YOUTUBE_COOKIES_PATH', '')
    
    try:
        import yt_dlp
        
        test_video_id = "jNQXAC9IVRw"
        video_url = f"https://www.youtube.com/watch?v={test_video_id}"
        
        print(f"\n📥 Testing download for: {test_video_id}")
        
        # Test info extraction first
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
        }
        
        if cookies_path and Path(cookies_path).exists():
            ydl_opts['cookiefile'] = cookies_path
            print(f"   Using cookies: {cookies_path}")
        
        print("   Extracting video info...")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            
            if info:
                print("   ✅ Video info extracted:")
                print(f"   Title: {info.get('title', 'N/A')}")
                print(f"   Duration: {info.get('duration', 'N/A')}s")
                print(f"   View count: {info.get('view_count', 'N/A')}")
                
                # Check for available formats
                formats = info.get('formats', [])
                video_formats = [f for f in formats if f.get('vcodec') != 'none']
                audio_formats = [f for f in formats if f.get('acodec') != 'none' and f.get('vcodec') == 'none']
                
                print(f"   Video formats: {len(video_formats)}")
                print(f"   Audio formats: {len(audio_formats)}")
                
                return True
            else:
                print("   ❌ No info extracted")
                return False
                
    except ImportError as e:
        print(f"❌ yt-dlp not installed: {e}")
        return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


def test_huggingface_vault(env: dict) -> bool:
    """Test HuggingFace Hub operations."""
    print("\n" + "="*60)
    print("4. Testing HuggingFace Vault")
    print("="*60)
    
    hf_token = env.get('HF_TOKEN', '')
    hf_dataset_id = env.get('HF_DATASET_ID', 'Rolaficus/pleiades-vault-test')
    
    if not hf_token:
        print("❌ HF_TOKEN not set")
        return False
    
    print(f"📋 Testing vault: {hf_dataset_id}")
    print("   Token: [redacted]")
    
    try:
        from huggingface_hub import HfApi
        
        api = HfApi(token=hf_token)
        
        # Test list files
        print("\n   Listing files in repo...")
        
        try:
            files = api.list_repo_files(
                repo_id=hf_dataset_id,
                repo_type="dataset"
            )
            
            print(f"   ✅ Found {len(files)} files")
            for f in files[:5]:
                print(f"   - {f}")
            if len(files) > 5:
                print(f"   ... and {len(files) - 5} more")
            
            return True
            
        except Exception as e:
            print(f"   ❌ List files failed: {e}")
            return False
            
    except ImportError as e:
        print(f"❌ huggingface_hub not installed: {e}")
        return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


def test_database_connection(env: dict) -> bool:
    """Test Neon database connection."""
    print("\n" + "="*60)
    print("5. Testing Neon Database Connection")
    print("="*60)
    
    db_url = env.get('DATABASE_URL', '')
    if not db_url:
        print("❌ DATABASE_URL not set")
        return False
    
    print("📋 Testing database...")
    print("   URL: [redacted]")
    
    try:
        import psycopg
        
        conn = psycopg.connect(db_url, connect_timeout=10)
        cur = conn.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"   ✅ Connected! PostgreSQL version: {version}")
        conn.close()
        return True
        
    except ImportError as e:
        print(f"❌ psycopg not installed: {e}")
        return False
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def main():
    print("="*60)
    print("PLEIADES YOUTUBE API DIAGNOSTIC TOOL")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("="*60)
    
    # Load environment
    env = load_env()
    if not env:
        print("❌ Failed to load environment")
        sys.exit(1)
    
    print("✅ Environment loaded")
    
    results = {}
    
    # Run all tests
    results['youtube_data_api'] = test_youtube_data_api(env)
    results['youtube_transcript'] = test_youtube_transcript_api(env)
    results['ytdlp_download'] = test_ytdlp_download(env)
    results['huggingface_vault'] = test_huggingface_vault(env)
    results['database'] = test_database_connection(env)
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {name}: {status}")
    
    print(f"\nTotal: {passed}/{total} passed")
    
    if passed == total:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n⚠️  Some tests failed. Check output above for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()