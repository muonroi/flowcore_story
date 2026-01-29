"""Turnstile Solver - Integration with Cloudflare Turnstile solving services.

This module provides integration with Turnstile solving services:
- 2Captcha integration
- Anti-Captcha integration
- CapSolver integration
- CapMonster integration
- Custom solver support
"""

from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Any

import httpx

from flowcore_story.utils.challenge_harvester import ChallengeResult, ChallengeType, SolveMethod
from flowcore_story.utils.logger import logger


class SolverService(Enum):
    """Supported solver services."""
    TWO_CAPTCHA = "2captcha"
    ANTI_CAPTCHA = "anticaptcha"
    CAP_SOLVER = "capsolver"
    CAP_MONSTER = "capmonster"
    CUSTOM = "custom"


class TurnstileSolver:
    """Solver for Cloudflare Turnstile challenges."""

    def __init__(
        self,
        service: SolverService,
        api_key: str,
        api_url: str | None = None,
    ):
        """Initialize Turnstile solver.

        Args:
            service: Solver service to use
            api_key: API key for the service
            api_url: Custom API URL (for custom service)
        """
        self.service = service
        self.api_key = api_key
        self.api_url = api_url

        # Service-specific URLs
        self._service_urls = {
            SolverService.TWO_CAPTCHA: "https://2captcha.com",
            SolverService.ANTI_CAPTCHA: "https://api.anti-captcha.com",
            SolverService.CAP_SOLVER: "https://api.capsolver.com",
            SolverService.CAP_MONSTER: "https://api.capmonster.cloud",
        }

        self.base_url = api_url or self._service_urls.get(service)

        logger.info(
            "[TurnstileSolver] Initialized with service: %s",
            service.value
        )

    async def solve(
        self,
        site_key: str,
        url: str,
        sitekey: str,
        context: Any = None,
        timeout: float = 120.0,
        **kwargs
    ) -> ChallengeResult:
        """Solve a Turnstile challenge.

        Args:
            site_key: Site identifier
            url: Page URL
            sitekey: Turnstile site key
            context: Playwright context (optional)
            timeout: Maximum time to wait for solution
            **kwargs: Additional service-specific parameters

        Returns:
            ChallengeResult with solve outcome
        """
        start_time = time.time()

        logger.info(
            "[TurnstileSolver] Solving Turnstile for %s (sitekey: %s)",
            site_key,
            sitekey[:20] + "..."
        )

        try:
            if self.service == SolverService.TWO_CAPTCHA:
                result = await self._solve_2captcha(url, sitekey, timeout, **kwargs)
            elif self.service == SolverService.ANTI_CAPTCHA:
                result = await self._solve_anticaptcha(url, sitekey, timeout, **kwargs)
            elif self.service == SolverService.CAP_SOLVER:
                result = await self._solve_capsolver(url, sitekey, timeout, **kwargs)
            elif self.service == SolverService.CAP_MONSTER:
                result = await self._solve_capmonster(url, sitekey, timeout, **kwargs)
            else:
                result = ChallengeResult(
                    success=False,
                    challenge_type=ChallengeType.CLOUDFLARE_TURNSTILE,
                    solve_method=SolveMethod.AUTOMATED_SOLVER,
                    error="Unsupported solver service"
                )

            result.solve_time = time.time() - start_time

            if result.success:
                logger.info(
                    "[TurnstileSolver] Solved in %.1fs",
                    result.solve_time
                )
            else:
                logger.warning(
                    "[TurnstileSolver] Failed: %s",
                    result.error
                )

            return result

        except Exception as e:
            logger.error(
                "[TurnstileSolver] Exception: %s",
                e,
                exc_info=True
            )

            return ChallengeResult(
                success=False,
                challenge_type=ChallengeType.CLOUDFLARE_TURNSTILE,
                solve_method=SolveMethod.AUTOMATED_SOLVER,
                solve_time=time.time() - start_time,
                error=str(e)
            )

    async def _solve_2captcha(
        self,
        url: str,
        sitekey: str,
        timeout: float,
        **kwargs
    ) -> ChallengeResult:
        """Solve using 2Captcha service.

        API Docs: https://2captcha.com/api-docs/cloudflare-turnstile

        Args:
            url: Page URL
            sitekey: Turnstile sitekey
            timeout: Timeout in seconds
            **kwargs: Additional parameters

        Returns:
            ChallengeResult
        """
        async with httpx.AsyncClient() as client:
            # Submit task
            submit_data = {
                "key": self.api_key,
                "method": "turnstile",
                "sitekey": sitekey,
                "pageurl": url,
                "json": 1,
            }

            # Add optional parameters
            if "action" in kwargs:
                submit_data["action"] = kwargs["action"]
            if "data" in kwargs:
                submit_data["data"] = kwargs["data"]

            try:
                response = await client.post(
                    f"{self.base_url}/in.php",
                    data=submit_data,
                    timeout=30.0
                )
                result = response.json()

                if result.get("status") != 1:
                    return ChallengeResult(
                        success=False,
                        challenge_type=ChallengeType.CLOUDFLARE_TURNSTILE,
                        solve_method=SolveMethod.AUTOMATED_SOLVER,
                        error=f"Submit failed: {result.get('request', 'Unknown error')}"
                    )

                task_id = result.get("request")

                # Poll for result
                start = time.time()
                while time.time() - start < timeout:
                    await asyncio.sleep(5.0)

                    result_response = await client.get(
                        f"{self.base_url}/res.php",
                        params={
                            "key": self.api_key,
                            "action": "get",
                            "id": task_id,
                            "json": 1,
                        },
                        timeout=30.0
                    )
                    result_data = result_response.json()

                    if result_data.get("status") == 1:
                        # Success
                        token = result_data.get("request")
                        return ChallengeResult(
                            success=True,
                            challenge_type=ChallengeType.CLOUDFLARE_TURNSTILE,
                            solve_method=SolveMethod.AUTOMATED_SOLVER,
                            tokens={"cf-turnstile-response": token},
                            ttl=3600.0,  # 1 hour
                            expires_at=time.time() + 3600.0,
                        )

                    if result_data.get("request") != "CAPCHA_NOT_READY":
                        # Error
                        return ChallengeResult(
                            success=False,
                            challenge_type=ChallengeType.CLOUDFLARE_TURNSTILE,
                            solve_method=SolveMethod.AUTOMATED_SOLVER,
                            error=f"Solve failed: {result_data.get('request')}"
                        )

                # Timeout
                return ChallengeResult(
                    success=False,
                    challenge_type=ChallengeType.CLOUDFLARE_TURNSTILE,
                    solve_method=SolveMethod.AUTOMATED_SOLVER,
                    error="Timeout waiting for solution"
                )

            except Exception as e:
                return ChallengeResult(
                    success=False,
                    challenge_type=ChallengeType.CLOUDFLARE_TURNSTILE,
                    solve_method=SolveMethod.AUTOMATED_SOLVER,
                    error=f"2Captcha error: {str(e)}"
                )

    async def _solve_anticaptcha(
        self,
        url: str,
        sitekey: str,
        timeout: float,
        **kwargs
    ) -> ChallengeResult:
        """Solve using Anti-Captcha service.

        API Docs: https://anti-captcha.com/apidoc/task-types/TurnstileTask

        Args:
            url: Page URL
            sitekey: Turnstile sitekey
            timeout: Timeout in seconds
            **kwargs: Additional parameters

        Returns:
            ChallengeResult
        """
        # Placeholder - similar to 2captcha but different API
        logger.warning("[TurnstileSolver] Anti-Captcha integration not fully implemented")
        return ChallengeResult(
            success=False,
            challenge_type=ChallengeType.CLOUDFLARE_TURNSTILE,
            solve_method=SolveMethod.AUTOMATED_SOLVER,
            error="Anti-Captcha not implemented"
        )

    async def _solve_capsolver(
        self,
        url: str,
        sitekey: str,
        timeout: float,
        **kwargs
    ) -> ChallengeResult:
        """Solve using CapSolver service.

        Args:
            url: Page URL
            sitekey: Turnstile sitekey
            timeout: Timeout in seconds
            **kwargs: Additional parameters

        Returns:
            ChallengeResult
        """
        # Placeholder
        logger.warning("[TurnstileSolver] CapSolver integration not fully implemented")
        return ChallengeResult(
            success=False,
            challenge_type=ChallengeType.CLOUDFLARE_TURNSTILE,
            solve_method=SolveMethod.AUTOMATED_SOLVER,
            error="CapSolver not implemented"
        )

    async def _solve_capmonster(
        self,
        url: str,
        sitekey: str,
        timeout: float,
        **kwargs
    ) -> ChallengeResult:
        """Solve using CapMonster service.

        Args:
            url: Page URL
            sitekey: Turnstile sitekey
            timeout: Timeout in seconds
            **kwargs: Additional parameters

        Returns:
            ChallengeResult
        """
        # Placeholder
        logger.warning("[TurnstileSolver] CapMonster integration not fully implemented")
        return ChallengeResult(
            success=False,
            challenge_type=ChallengeType.CLOUDFLARE_TURNSTILE,
            solve_method=SolveMethod.AUTOMATED_SOLVER,
            error="CapMonster not implemented"
        )


def create_solver(
    service: str = "2captcha",
    api_key: str | None = None,
    **kwargs
) -> TurnstileSolver | None:
    """Create a Turnstile solver instance.

    Args:
        service: Service name ("2captcha", "anticaptcha", etc.)
        api_key: API key for the service
        **kwargs: Additional arguments

    Returns:
        TurnstileSolver instance or None if api_key not provided
    """
    if not api_key:
        logger.warning("[TurnstileSolver] No API key provided, solver disabled")
        return None

    try:
        service_enum = SolverService(service.lower())
    except ValueError:
        logger.error("[TurnstileSolver] Unknown service: %s", service)
        return None

    return TurnstileSolver(
        service=service_enum,
        api_key=api_key,
        **kwargs
    )


async def register_solver_with_harvester(
    harvester,
    solver: TurnstileSolver,
) -> None:
    """Register Turnstile solver with challenge harvester.

    Args:
        harvester: ChallengeHarvester instance
        solver: TurnstileSolver instance
    """
    async def solve_wrapper(site_key, url, context, **kwargs):
        # Extract sitekey from page or kwargs
        sitekey = kwargs.get("sitekey")

        if not sitekey and context:
            # Try to extract from page
            try:
                page = await context.new_page()
                await page.goto(url, wait_until="domcontentloaded")

                sitekey = await page.evaluate("""
                    () => {
                        const turnstile = document.querySelector('[data-sitekey]');
                        return turnstile ? turnstile.getAttribute('data-sitekey') : null;
                    }
                """)

                await page.close()
            except Exception as e:
                logger.warning("[TurnstileSolver] Could not extract sitekey: %s", e)

        if not sitekey:
            return ChallengeResult(
                success=False,
                challenge_type=ChallengeType.CLOUDFLARE_TURNSTILE,
                solve_method=SolveMethod.AUTOMATED_SOLVER,
                error="Could not determine Turnstile sitekey"
            )

        return await solver.solve(site_key, url, sitekey, context, **kwargs)

    harvester.register_solver(
        ChallengeType.CLOUDFLARE_TURNSTILE,
        solve_wrapper
    )

    logger.info("[TurnstileSolver] Registered with harvester")


__all__ = [
    "SolverService",
    "TurnstileSolver",
    "create_solver",
    "register_solver_with_harvester",
]

