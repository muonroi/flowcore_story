from bs4 import Comment


# Step 1: Clean HTML tags
def clean_chapter_content(chapter_div):
    # Xóa các thẻ script, style
    for tag in chapter_div.find_all(['script', 'style']):
        tag.decompose()
    # Chỉ xóa nếu KHÔNG phải là chính chapter_div
    for tag in chapter_div.find_all(True):
        if tag is chapter_div:
            continue
        tag_classes = tag.get('class') or []
        if any(cls for cls in tag_classes if cls in ['ads', 'notice', 'box-notice'] or 'ads' in cls or 'notice' in cls or 'box-notice' in cls):
            tag.decompose()
    # Xóa comment
    for element in chapter_div(text=lambda text: isinstance(text, Comment)):
        element.extract()
    return chapter_div


def ensure_sources_priority(sources: list) -> list:
    """Thêm priority mặc định vào các nguồn (nếu chưa có)."""
    for source in sources:
        if "priority" not in source or source["priority"] is None:
            source["priority"] = 100
    return sources
