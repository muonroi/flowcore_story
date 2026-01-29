"""
Adapter regression smoke tests runnable inside a container.

This script validates two key regressions:
1. TangThuVien chapter validation no longer rejects valid chapters when navigation
   headers mention different chapter numbers.
2. TruyenCom adapter now reports total_chapters_on_site alongside total_chapters.

Run with: python scripts/adapter_fix_smoke.py
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

from flowcore_story.adapters.tangthuvien_adapter import TangThuVienAdapter
from flowcore_story.adapters.truyencom_adapter import TruyenComAdapter


def _test_tangthuvien_content_validation() -> None:
    adapter = TangThuVienAdapter()
    fake_content = """
        <div class="chapter-c-content">
            <p>Chương 8: (navigation link)</p>
            <p>Nội dung ...</p>
            <p>Chương 807: Nội dung chính xác</p>
        </div>
    """
    parsed = {"content": fake_content, "title": "Chương 807"}
    assert adapter._content_looks_valid(parsed, "Chương 807"), "TangThuVien validation rejected valid chapter content"


async def _test_truyencom_total_chapters() -> None:
    adapter = TruyenComAdapter()
    fake_story_html = """
        <html>
            <body>
                <div class="info">
                    <h1 class="title" itemprop="name">Giả Lập</h1>
                    <a itemprop="author">Tác Giả</a>
                </div>
                <div class="list list-truyen">
                    <div class="row" itemtype="Book">
                        <h3 class="truyen-title"><a href="/gia-lap/chuong-1/">Chương 1</a></h3>
                    </div>
                    <div class="row" itemtype="Book">
                        <h3 class="truyen-title"><a href="/gia-lap/chuong-2/">Chương 2</a></h3>
                    </div>
                </div>
                <ul class="list-chapter">
                    <li><a href="/gia-lap/chuong-1/">Chương 1</a></li>
                    <li><a href="/gia-lap/chuong-2/">Chương 2</a></li>
                </ul>
            </body>
        </html>
    """

    async def fake_make_request(url, site_key, **kwargs):
        return SimpleNamespace(text=fake_story_html)

    patch_target = "flowcore_story.adapters.truyencom_adapter.make_request"
    with patch(patch_target, side_effect=fake_make_request):
        details = await adapter._get_story_details_internal("https://truyencom.com/gia-lap/")
    assert details, "TruyenCom adapter failed to parse fake story HTML"
    assert details["total_chapters_on_site"] == 2, "total_chapters_on_site mismatch"
    assert details["total_chapters"] == 2, "total_chapters mismatch"


async def main() -> int:
    print("[+] Running TangThuVien chapter validation check…")
    _test_tangthuvien_content_validation()
    print("[✓] TangThuVien validation passed")

    print("[+] Running TruyenCom total chapters check…")
    await _test_truyencom_total_chapters()
    print("[✓] TruyenCom metadata passed")

    print("[✓] All adapter smoke tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
