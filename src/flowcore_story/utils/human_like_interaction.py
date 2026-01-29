"""Human-Like Interaction - Simulate realistic human behavior in headful browsers.

This module provides realistic human interaction patterns for challenge solving:
- Mouse movement with curves and randomness
- Typing with realistic delays and errors
- Scrolling with natural patterns
- Wait times with human variability
- Detection of human completion
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any

from flowcore_story.utils.challenge_harvester import ChallengeResult, ChallengeType, SolveMethod
from flowcore_story.utils.logger import logger


class HumanLikeInteraction:
    """Simulates human-like interactions for realistic browser behavior."""

    @staticmethod
    async def move_mouse_to(
        page,
        x: float,
        y: float,
        duration: float = 0.5,
        curve_intensity: float = 0.3,
    ) -> None:
        """Move mouse to position with realistic curve.

        Args:
            page: Playwright page
            x: Target x coordinate
            y: Target y coordinate
            duration: Movement duration in seconds
            curve_intensity: How curved the movement is (0-1)
        """
        try:
            # Get current mouse position (approximate)
            current_x, current_y = 0, 0  # Default to origin

            # Calculate bezier curve points
            steps = int(duration * 100)  # 100 steps per second

            # Control point for curve
            mid_x = (current_x + x) / 2 + random.uniform(-50, 50) * curve_intensity
            mid_y = (current_y + y) / 2 + random.uniform(-50, 50) * curve_intensity

            for i in range(steps):
                t = i / steps

                # Quadratic bezier curve
                curr_x = (1-t)**2 * current_x + 2*(1-t)*t * mid_x + t**2 * x
                curr_y = (1-t)**2 * current_y + 2*(1-t)*t * mid_y + t**2 * y

                # Add small random jitter
                curr_x += random.uniform(-1, 1)
                curr_y += random.uniform(-1, 1)

                await page.mouse.move(curr_x, curr_y)
                await asyncio.sleep(duration / steps)

        except Exception as e:
            logger.debug("[HumanLike] Mouse move failed: %s", e)

    @staticmethod
    async def human_type(
        page,
        selector: str,
        text: str,
        mistakes_probability: float = 0.05,
    ) -> None:
        """Type text with human-like delays and occasional mistakes.

        Args:
            page: Playwright page
            selector: Element selector
            text: Text to type
            mistakes_probability: Probability of making a typo (0-1)
        """
        try:
            await page.focus(selector)

            for char in text:
                # Random delay between keystrokes (50-200ms)
                delay = random.uniform(50, 200)

                # Occasionally make a typo
                if random.random() < mistakes_probability:
                    # Type wrong character
                    wrong_char = chr(ord(char) + random.choice([-1, 1]))
                    await page.keyboard.type(wrong_char, delay=delay)

                    # Realize mistake and backspace
                    await asyncio.sleep(random.uniform(100, 300) / 1000)
                    await page.keyboard.press("Backspace")
                    await asyncio.sleep(random.uniform(50, 150) / 1000)

                # Type correct character
                await page.keyboard.type(char, delay=delay)

                # Occasional longer pause (thinking)
                if random.random() < 0.1:
                    await asyncio.sleep(random.uniform(300, 800) / 1000)

        except Exception as e:
            logger.debug("[HumanLike] Typing failed: %s", e)

    @staticmethod
    async def human_click(
        page,
        selector: str,
        move_to_element: bool = True,
    ) -> None:
        """Click element with human-like mouse movement.

        Args:
            page: Playwright page
            selector: Element selector
            move_to_element: Whether to move mouse to element first
        """
        try:
            element = await page.query_selector(selector)
            if not element:
                logger.warning("[HumanLike] Element not found: %s", selector)
                return

            # Get element bounding box
            box = await element.bounding_box()
            if not box:
                logger.warning("[HumanLike] Could not get element bounds: %s", selector)
                return

            # Click at random point within element
            click_x = box["x"] + random.uniform(box["width"] * 0.3, box["width"] * 0.7)
            click_y = box["y"] + random.uniform(box["height"] * 0.3, box["height"] * 0.7)

            if move_to_element:
                # Move mouse to element with curve
                await HumanLikeInteraction.move_mouse_to(
                    page,
                    click_x,
                    click_y,
                    duration=random.uniform(0.3, 0.8)
                )

            # Small delay before click (human reaction time)
            await asyncio.sleep(random.uniform(0.05, 0.15))

            # Click with random duration
            await page.mouse.click(click_x, click_y)

        except Exception as e:
            logger.debug("[HumanLike] Click failed: %s", e)

    @staticmethod
    async def human_scroll(
        page,
        amount: int = 500,
        smooth: bool = True,
    ) -> None:
        """Scroll page with human-like pattern.

        Args:
            page: Playwright page
            amount: Scroll amount in pixels (positive = down, negative = up)
            smooth: Whether to scroll smoothly
        """
        try:
            if smooth:
                # Smooth scroll in steps
                steps = abs(amount) // 50
                step_amount = amount / steps if steps > 0 else amount

                for _ in range(steps):
                    await page.mouse.wheel(0, step_amount)
                    await asyncio.sleep(random.uniform(0.01, 0.03))

                    # Occasional pause
                    if random.random() < 0.2:
                        await asyncio.sleep(random.uniform(0.1, 0.3))
            else:
                await page.mouse.wheel(0, amount)

        except Exception as e:
            logger.debug("[HumanLike] Scroll failed: %s", e)

    @staticmethod
    async def random_human_behavior(
        page,
        duration: float = 2.0,
    ) -> None:
        """Perform random human-like behaviors.

        Args:
            page: Playwright page
            duration: Duration to perform behaviors
        """
        end_time = time.time() + duration

        behaviors = [
            lambda: HumanLikeInteraction.human_scroll(page, random.randint(50, 200)),
            lambda: HumanLikeInteraction.move_mouse_to(
                page,
                random.uniform(100, 800),
                random.uniform(100, 600),
                duration=random.uniform(0.5, 1.5)
            ),
        ]

        try:
            while time.time() < end_time:
                behavior = random.choice(behaviors)
                await behavior()
                await asyncio.sleep(random.uniform(0.5, 2.0))
        except Exception as e:
            logger.debug("[HumanLike] Random behavior failed: %s", e)


async def wait_for_human_solve(
    challenge_type: ChallengeType,
    site_key: str,
    url: str,
    context: Any,
    timeout: float = 120.0,
    check_interval: float = 1.0,
    **kwargs
) -> ChallengeResult:
    """Wait for human to solve challenge in headful browser.

    Args:
        challenge_type: Type of challenge
        site_key: Site identifier
        url: Challenge URL
        context: Playwright browser context
        timeout: Maximum wait time in seconds
        check_interval: How often to check for completion
        **kwargs: Additional arguments

    Returns:
        ChallengeResult with solve outcome
    """
    start_time = time.time()

    logger.info(
        "[HumanSolve] Waiting for human to solve %s challenge for %s (timeout: %.0fs)",
        challenge_type.value,
        site_key,
        timeout
    )

    try:
        # Create new page in context
        page = await context.new_page()

        # Navigate to URL
        await page.goto(url, wait_until="domcontentloaded")

        # Wait a bit for challenge to appear
        await asyncio.sleep(2.0)

        # Show instruction to user
        await _show_solve_instruction(page, challenge_type, site_key)

        # Poll for completion
        attempt = 0
        while True:
            attempt += 1
            elapsed = time.time() - start_time

            if elapsed >= timeout:
                logger.warning(
                    "[HumanSolve] Timeout waiting for human solve (%.0fs elapsed)",
                    elapsed
                )
                return ChallengeResult(
                    success=False,
                    challenge_type=challenge_type,
                    solve_method=SolveMethod.HUMAN_WORKFLOW,
                    attempts=attempt,
                    solve_time=elapsed,
                    error="Timeout waiting for human solve"
                )

            # Check if challenge is solved
            is_solved = await _check_challenge_solved(page, challenge_type)

            if is_solved:
                # Get clearance cookies
                cookies = await context.cookies()

                # Get any tokens from page
                tokens = await _extract_challenge_tokens(page, challenge_type)

                # Estimate TTL based on challenge type
                ttl = _estimate_clearance_ttl(challenge_type)

                logger.info(
                    "[HumanSolve] Challenge solved by human in %.1fs",
                    elapsed
                )

                await page.close()

                return ChallengeResult(
                    success=True,
                    challenge_type=challenge_type,
                    solve_method=SolveMethod.HUMAN_WORKFLOW,
                    cookies=cookies,
                    tokens=tokens,
                    attempts=attempt,
                    solve_time=elapsed,
                    ttl=ttl,
                    expires_at=time.time() + ttl,
                )

            # Wait before next check
            await asyncio.sleep(check_interval)

    except Exception as e:
        logger.error(
            "[HumanSolve] Error during human solve: %s",
            e,
            exc_info=True
        )

        return ChallengeResult(
            success=False,
            challenge_type=challenge_type,
            solve_method=SolveMethod.HUMAN_WORKFLOW,
            solve_time=time.time() - start_time,
            error=str(e)
        )


async def _show_solve_instruction(
    page,
    challenge_type: ChallengeType,
    site_key: str,
) -> None:
    """Show instruction overlay for human solver.

    Args:
        page: Playwright page
        challenge_type: Type of challenge
        site_key: Site identifier
    """
    try:
        instruction_html = f"""
        <div id="human-solve-instruction" style="
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px;
            font-family: Arial, sans-serif;
            font-size: 16px;
            z-index: 999999;
            text-align: center;
            box-shadow: 0 2px 10px rgba(0,0,0,0.3);
        ">
            <strong>🤖 Challenge Harvester</strong><br/>
            Please solve the <strong>{challenge_type.value}</strong> challenge for <strong>{site_key}</strong><br/>
            <small>This window will automatically detect when you're done</small>
        </div>
        """

        await page.evaluate("""
            (html) => {
                const div = document.createElement('div');
                div.innerHTML = html;
                document.body.appendChild(div.firstElementChild);
            }
        """, instruction_html)
    except Exception as e:
        logger.debug("[HumanSolve] Could not show instruction: %s", e)


async def _check_challenge_solved(
    page,
    challenge_type: ChallengeType,
) -> bool:
    """Check if challenge has been solved.

    Args:
        page: Playwright page
        challenge_type: Type of challenge

    Returns:
        True if solved
    """
    try:
        content = await page.content()

        # Check for common success indicators
        success_indicators = [
            "challenge passed",
            "verification complete",
            "access granted",
        ]

        content_lower = content.lower()
        for indicator in success_indicators:
            if indicator in content_lower:
                return True

        # Check if challenge elements are gone
        from flowcore_story.utils.challenge_harvester import ChallengeDetector
        has_challenge = ChallengeDetector.has_challenge(content)

        if not has_challenge:
            # Challenge is no longer present - likely solved
            return True

        # Check for specific challenge completion patterns
        if challenge_type == ChallengeType.CLOUDFLARE_TURNSTILE:
            # Check for Turnstile success token
            try:
                token = await page.evaluate("""
                    () => {
                        const input = document.querySelector('input[name="cf-turnstile-response"]');
                        return input ? input.value : null;
                    }
                """)
                if token:
                    return True
            except Exception:
                pass

        return False

    except Exception as e:
        logger.debug("[HumanSolve] Error checking if solved: %s", e)
        return False


async def _extract_challenge_tokens(
    page,
    challenge_type: ChallengeType,
) -> dict[str, str]:
    """Extract challenge tokens from page.

    Args:
        page: Playwright page
        challenge_type: Type of challenge

    Returns:
        Dict of token name to value
    """
    tokens = {}

    try:
        if challenge_type == ChallengeType.CLOUDFLARE_TURNSTILE:
            # Extract Turnstile response token
            token = await page.evaluate("""
                () => {
                    const input = document.querySelector('input[name="cf-turnstile-response"]');
                    return input ? input.value : null;
                }
            """)
            if token:
                tokens["cf-turnstile-response"] = token

        elif challenge_type == ChallengeType.RECAPTCHA_V2:
            # Extract reCAPTCHA response
            token = await page.evaluate("""
                () => {
                    const textarea = document.querySelector('textarea[name="g-recaptcha-response"]');
                    return textarea ? textarea.value : null;
                }
            """)
            if token:
                tokens["g-recaptcha-response"] = token

        # Add more token extraction logic as needed

    except Exception as e:
        logger.debug("[HumanSolve] Error extracting tokens: %s", e)

    return tokens


def _estimate_clearance_ttl(challenge_type: ChallengeType) -> float:
    """Estimate TTL for clearance based on challenge type.

    Args:
        challenge_type: Type of challenge

    Returns:
        Estimated TTL in seconds
    """
    ttl_map = {
        ChallengeType.CLOUDFLARE_TURNSTILE: 3600.0,  # 1 hour
        ChallengeType.CLOUDFLARE_UAM: 1800.0,        # 30 minutes
        ChallengeType.RECAPTCHA_V2: 7200.0,          # 2 hours
        ChallengeType.RECAPTCHA_V3: 7200.0,          # 2 hours
        ChallengeType.HCAPTCHA: 3600.0,              # 1 hour
    }

    return ttl_map.get(challenge_type, 3600.0)  # Default 1 hour


__all__ = [
    "HumanLikeInteraction",
    "wait_for_human_solve",
]

