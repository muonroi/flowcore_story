# Adding a new site adapter

This project now discovers adapters automatically. Follow the steps below to
integrate a new source without touching the core factory logic.

## 1. Create a module

Create a new file in `adapters/` whose name ends with `_adapter.py` (for
example `my_site_adapter.py`). The automatic discovery system scans for files
matching this naming convention.

Inside the module implement a class that inherits from
`adapters.base_site_adapter.BaseSiteAdapter` and define a unique `site_key`
class attribute:

```python
from storyflow_core.adapters.base_site_adapter import BaseSiteAdapter


class MySiteAdapter(BaseSiteAdapter):
    site_key = "mysite"

    async def get_genres(self):
        ...
```

Every abstract method declared on `BaseSiteAdapter` must be implemented. The
`site_key` attribute is mandatory and is used by the factory when instantiating
adapters.

## 2. Provide optional configuration

Per-site configuration values can be placed in `config/sites/<site_key>.json`.
Adapters can read these values via `utils.site_config.load_site_config`. Use
this mechanism to store selectors, API endpoints, or other tweakable settings so
that adding new sites requires minimal code changes. The loader also supports an
override through the `SITE_CONFIG_DIR` environment variable if you need to point
to a different configuration directory at runtime.

A minimal configuration file might look like:

```json
{
  "display_name": "My Site",
  "chapter_page_size": 42
}
```

## 3. Instantiate the adapter

Callers can retrieve the adapter instance by invoking
`adapters.factory.get_adapter("mysite")`. The factory relies on the automatic
registry and therefore requires no further edits when new adapters are added.
You can inspect the available site keys through
`adapters.factory.available_site_keys()`.

By following these steps, new adapters can be introduced without modifying the
core crawling code, making the system easier to extend and maintain.
