from flowcore_story.config.config import GLOBAL_PROXY_PASSWORD, GLOBAL_PROXY_USERNAME
from flowcore_story.config.proxy_provider import get_proxy_url


def get_proxy_for_site(site_key: str | None) -> str | None:
    return get_proxy_url(
        username=GLOBAL_PROXY_USERNAME,
        password=GLOBAL_PROXY_PASSWORD,
        site_key=site_key,
    )
