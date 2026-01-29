import json
import re
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger("storyflow.next_data_parser")

class NextDataParser:
    """
    Parser for Next.js App Router stream data (__next_f.push).
    Used to extract hidden movie links and metadata from RSC chunks.
    """

    XOR_KEY = "mySecretKey2024"
    
    # Regex to find JSON-like objects in RSC chunks
    # Example: 1:I["...",["..."],""] or 2:{"url":"..."}
    CHUNK_PATTERN = re.compile(r'^[0-9a-f]+:(.*)$', re.MULTILINE)

    @staticmethod
    def decode_xor_hex(hex_str: str, key: str) -> str:
        if not hex_str or not key:
            return ""

        cleaned = re.sub(r'[^0-9a-fA-F]', '', hex_str)
        if len(cleaned) < 2:
            return ""
        if len(cleaned) % 2 == 1:
            cleaned = cleaned[:-1]

        try:
            data = bytes.fromhex(cleaned)
        except ValueError:
            return ""

        key_bytes = key.encode("utf-8")
        decoded = bytes(
            byte ^ key_bytes[i % len(key_bytes)]
            for i, byte in enumerate(data)
        )
        try:
            return decoded.decode("utf-8")
        except UnicodeDecodeError:
            return decoded.decode("utf-8", errors="ignore")
    
    @staticmethod
    def extract_m3u8_links(html_content: str) -> List[str]:
        """
        Scan HTML content for __next_f.push calls and extract m3u8 URLs.
        """
        links = []
        
        # 1. Direct Regex on HTML (Fastest)
        # Look for m3u8 links in any context (scripts, data attributes, etc)
        direct_matches = re.findall(r'https?://[^\s"\'\\]+?\.m3u8[^\s"\'\\]*', html_content)
        links.extend(direct_matches)
        
        # 2. Scan Next.js stream pushes
        pushes = re.findall(r'self\.__next_f\.push\(\[(.*?)\]\)', html_content, re.DOTALL)
        for push in pushes:
            # Look for links even if escaped
            # Example: \"https:\/\/...\"
            escaped_matches = re.findall(r'https?:\\?\/\\?\/[^\s"\'\\]+?\.m3u8[^\s"\'\\]*', push)
            for m in escaped_matches:
                links.append(m.replace('\\/', '/'))
                
            # Try to find JSON chunks and scan them
            try:
                # Basic JSON-like structures inside pushes
                json_like = re.findall(r'\{.*\}', push)
                for j in json_like:
                    try:
                        data = json.loads(j)
                        # Recursive search in dict
                        def find_links(obj):
                            if isinstance(obj, str):
                                if ".m3u8" in obj: links.append(obj)
                            elif isinstance(obj, dict):
                                for v in obj.values(): find_links(v)
                            elif isinstance(obj, list):
                                for v in obj: find_links(v)
                        find_links(data)
                    except: pass
            except: pass

        # 3. Encrypted m3u8 URLs (XOR hex)
        encrypted_matches = re.findall(
            r'encrypted_url["\']?\s*[:=]\s*\\?["\']?([a-fA-F0-9]+)',
            html_content
        )
        for encrypted in encrypted_matches:
            decoded = NextDataParser.decode_xor_hex(encrypted, NextDataParser.XOR_KEY)
            if not decoded:
                continue

            normalized = decoded.replace('\\/', '/')
            if "\\u" in normalized or "\\x" in normalized:
                try:
                    normalized = normalized.encode("utf-8").decode("unicode_escape")
                except UnicodeDecodeError:
                    pass

            decoded_matches = re.findall(
                r'https?://[^\s"\'\\]+?\.m3u8[^\s"\'\\]*',
                normalized
            )
            if decoded_matches:
                links.extend(decoded_matches)
            elif ".m3u8" in normalized:
                links.append(normalized)
                
        # Deduplicate and clean
        unique_links = list(set(links))
        return [l.strip().strip('\\"') for l in unique_links]

    @staticmethod
    def parse_rsc_payload(html_content: str) -> List[Dict[str, Any]]:
        """
        Attempts to reconstruct structured data from RSC chunks.
        """
        results = []
        pushes = re.findall(r'self\.__next_f\.push\(\[(.*?)\]\)', html_content, re.DOTALL)
        
        for push in pushes:
            try:
                # Extract the string content (usually the second element in the array)
                # Format is usually [1, "data"]
                match = re.search(r'^[0-9]+\s*,\s*"(.*?)"$', push.strip(), re.DOTALL)
                if not match:
                    continue
                    
                chunk = match.group(1).encode().decode('unicode_escape')
                
                # Look for JSON objects inside the chunk
                json_matches = re.findall(r'\{.*?\}', chunk)
                for j in json_matches:
                    try:
                        results.append(json.loads(j))
                    except:
                        pass
            except:
                continue
        return results

def extract_movie_source(html: str) -> Optional[str]:
    """
    High-level helper to find the best movie source from Rophim/NextJS HTML.
    """
    parser = NextDataParser()
    links = parser.extract_m3u8_links(html)
    
    # Priority: m3u8 links that look like primary streams
    if links:
        # Filter out common ad streams or low-quality thumbnails if necessary
        for link in links:
            if "playlist.m3u8" in link or "master.m3u8" in link:
                return link
        return links[0]
        
    return None
