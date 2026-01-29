"""
Health check utilities for database and state storage.
Provides comprehensive status information for monitoring.
"""

from typing import Any

from flowcore_story.utils.logger import logger


async def check_db_health() -> dict[str, Any]:
    """
    Check database health and return comprehensive metrics.

    Returns:
        Dict containing health status, circuit breaker state, pool info,
        batch writer stats, and metrics.
    """
    health = {
        "status": "unknown",
        "circuit_breaker": None,
        "pool": None,
        "batch_writer": None,
        "metrics": None,
    }

    # Circuit breaker state
    try:
        from flowcore_story.storage.db_circuit_breaker import get_circuit_breaker
        breaker = get_circuit_breaker()
        health["circuit_breaker"] = breaker.get_state()
    except Exception as e:
        logger.warning(f"[Health] Cannot get circuit breaker state: {e}")
        health["circuit_breaker"] = {"error": str(e)}

    # Pool status
    try:
        from flowcore_story.storage.db_pool import get_db_pool
        pool = await get_db_pool()

        if pool:
            health["pool"] = {
                "available": True,
                "size": pool.get_size(),
                "idle": pool.get_idle_size(),
                "busy": pool.get_size() - pool.get_idle_size(),
            }
            health["status"] = "healthy"
        else:
            health["pool"] = {
                "available": False,
                "reason": "Pool not initialized or connection failed"
            }
            health["status"] = "degraded"
    except Exception as e:
        logger.warning(f"[Health] Cannot get pool status: {e}")
        health["pool"] = {"error": str(e)}
        health["status"] = "degraded"

    # Batch writer stats
    try:
        from flowcore_story.storage.db_batch_writer import get_batch_writer
        writer = await get_batch_writer()
        health["batch_writer"] = writer.get_stats()
    except Exception as e:
        logger.debug(f"[Health] Batch writer not available: {e}")
        health["batch_writer"] = {"available": False}

    # State save metrics
    try:
        from flowcore_story.storage.state_metrics import get_state_metrics
        metrics = get_state_metrics()
        health["metrics"] = metrics.get_summary()
    except Exception as e:
        logger.debug(f"[Health] Metrics not available: {e}")
        health["metrics"] = {"available": False}

    return health


async def check_db_simple() -> dict[str, Any]:
    """
    Simple health check - just connectivity.

    Returns:
        Dict with status and basic info.
    """
    try:
        from flowcore_story.storage.db_pool import get_db_pool
        pool = await get_db_pool()

        if pool:
            # Try a simple query
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")

            return {
                "status": "healthy",
                "message": "Database connection OK"
            }
        else:
            return {
                "status": "degraded",
                "message": "Database pool not available (using file storage)"
            }
    except Exception as e:
        return {
            "status": "unhealthy",
            "message": f"Database error: {e}"
        }


def get_optimization_summary() -> dict[str, Any]:
    """
    Get summary of all optimizations and their status.

    Returns:
        Dict showing which optimizations are enabled.
    """
    import os

    return {
        "debounce_time": float(os.getenv("STATE_SAVE_DEBOUNCE", "30.0")),
        "dirty_tracking_enabled": os.getenv("STATE_ENABLE_DIRTY_TRACKING", "true").lower() == "true",
        "batch_writer_enabled": os.getenv("STATE_USE_BATCH_WRITER", "true").lower() == "true",
        "use_file_state": os.getenv("USE_FILE_STATE", "false").lower() == "true",
        "connection_retries": int(os.getenv("POSTGRES_STATE_CONNECT_RETRIES", "5")),
        "circuit_breaker": {
            "failure_threshold": int(os.getenv("POSTGRES_STATE_CIRCUIT_FAILURE_THRESHOLD", "5")),
            "recovery_timeout": float(os.getenv("POSTGRES_STATE_CIRCUIT_RECOVERY_TIMEOUT", "60.0")),
        },
        "batch_config": {
            "flush_interval": float(os.getenv("STATE_BATCH_FLUSH_INTERVAL", "10.0")),
            "max_batch_size": int(os.getenv("STATE_BATCH_MAX_SIZE", "50")),
        }
    }
