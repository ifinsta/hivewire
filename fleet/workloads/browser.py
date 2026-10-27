"""Real browser journey for an explicitly authorized synthetic staging account.

Install Playwright and its browser separately in the isolated load-worker
environment. This module does not bypass CAPTCHA, MFA, or anti-bot protections.
"""
from __future__ import annotations

import os
import sys
from urllib.parse import urlparse


def approve_origin(origin: str, allowed: str, approved: str) -> str:
    if approved != "yes":
        raise ValueError("HWP_TEST_APPROVED=yes is required for browser workloads")
    value = urlparse(origin)
    authorized = [part.strip().rstrip("/") for part in allowed.split(",") if part.strip()]
    if value.scheme not in ("https", "http") or not value.netloc or value.username or value.password:
        raise ValueError("test origin is malformed")
    if origin.rstrip("/") not in authorized:
        raise ValueError("test origin is not present in the explicit authorization allowlist")
    if value.scheme != "https" and value.hostname not in ("localhost", "127.0.0.1"):
        raise ValueError("insecure test origins are limited to loopback")
    if value.path not in ("", "/") or value.params or value.query or value.fragment:
        raise ValueError("origin must not include credentials, path, query or fragment")
    return origin.rstrip("/")


def run() -> int:
    origin = approve_origin(os.getenv("HWP_TEST_ORIGIN", ""),
                            os.getenv("HWP_ALLOWED_ORIGINS", ""),
                            os.getenv("HWP_TEST_APPROVED", ""))
    email = os.environ.get("HWP_SYNTHETIC_EMAIL")
    password = os.environ.get("HWP_SYNTHETIC_PASSWORD")
    if not email or not password:
        raise ValueError("dedicated synthetic account credentials are required")
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("Install playwright and provision Chromium on the worker") from exc
    with sync_playwright() as browser_driver:
        browser = browser_driver.chromium.launch(headless=True)
        context = browser.new_context()
        # Reject any cross-origin navigation or third-party request unless explicitly
        # approved. This may require extending the allowlist for known staging CDNs.
        allowed = {urlparse(a.strip()).netloc for a in
                   os.environ["HWP_ALLOWED_ORIGINS"].split(",") if a.strip()}
        def route_request(route):
            target = urlparse(route.request.url)
            if target.scheme in ("http", "https") and target.netloc not in allowed:
                route.abort()
            else:
                route.continue_()
        context.route("**/*", route_request)
        page = context.new_page()
        try:
            page.goto(origin, wait_until="domcontentloaded", timeout=15000)
            page.get_by_role("link", name="Log in").click(timeout=10000)
            page.get_by_label("Email").fill(email)
            page.get_by_label("Password").fill(password)
            page.get_by_role("button", name="Sign in").click()
            page.get_by_role("link", name="Account").wait_for(state="visible", timeout=15000)
            page.get_by_role("searchbox").fill("Laptop")
            page.get_by_role("button", name="Search").click()
            page.get_by_test_id("search-results").wait_for(state="visible", timeout=15000)
            print("HWP_BROWSER_JOURNEY_OK")
        finally:
            context.close()
            browser.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except (ValueError, RuntimeError) as error:
        print("browser-workload: " + str(error), file=sys.stderr)
        raise SystemExit(2) from error
