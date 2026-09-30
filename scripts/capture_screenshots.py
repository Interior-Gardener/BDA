"""Capture dashboard screenshots, an animated GIF tour and a demo video.

Requires the dashboard to be running (python run_dashboard.py) and the
dev-only packages:  pip install playwright pillow  &&  playwright install chromium

    python scripts/capture_screenshots.py            # -> docs/screenshots, docs/demo
"""

import argparse
import asyncio
from pathlib import Path

from PIL import Image
from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parent.parent
SHOTS = ROOT / "docs" / "screenshots"
DEMO = ROOT / "docs" / "demo"

PAGES = [
    ("overview", "01_overview"), ("sales", "02_sales"), ("products", "03_products"),
    ("behaviour", "04_behaviour"), ("segments", "05_segmentation"), ("churn", "06_churn"),
    ("recommendations", "07_recommendations"), ("basket", "08_market_basket"),
    ("forecast", "09_forecast"), ("customer/{cid}", "10_customer360"), ("pipeline", "11_pipeline"),
]


async def main(base, cid, video):
    (SHOTS / "full").mkdir(parents=True, exist_ok=True)
    DEMO.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        # ---------------------------------------------------------- screenshots (dark + light)
        for theme in ("dark", "light"):
            ctx = await browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
            page = await ctx.new_page()
            await page.goto(base)
            await page.evaluate(f"localStorage.setItem('ss-theme', '{theme}')")
            for route, name in PAGES:
                await page.goto(f"{base}/#/{route.format(cid=cid)}")
                await page.reload()
                await page.wait_for_timeout(2600)
                suffix = "" if theme == "dark" else "_light"
                await page.screenshot(path=str(SHOTS / f"{name}{suffix}.png"))
                if theme == "light":             # full-length pages (used in the report)
                    await page.add_style_tag(content=".topbar{position:static!important}")
                    await page.screenshot(path=str(SHOTS / "full" / f"{name}_full.png"), full_page=True)
            await ctx.close()

        # ---------------------------------------------------------- GIF tour (dark)
        ctx = await browser.new_context(viewport={"width": 1440, "height": 900})
        page = await ctx.new_page()
        await page.goto(base)
        await page.evaluate("localStorage.setItem('ss-theme', 'dark')")
        frames = []
        for route, _ in PAGES:
            await page.goto(f"{base}/#/{route.format(cid=cid)}")
            await page.wait_for_timeout(2200)
            for y in (0, 700):
                await page.evaluate(f"window.scrollTo(0, {y})")
                await page.wait_for_timeout(500)
                path = DEMO / f"_frame{len(frames):03d}.png"
                await page.screenshot(path=str(path))
                frames.append(path)
        await ctx.close()
        imgs = [Image.open(f).convert("RGB").resize((960, 600), Image.LANCZOS) for f in frames]
        pal = [im.quantize(colors=160, method=Image.Quantize.MEDIANCUT) for im in imgs]
        pal[0].save(DEMO / "shopsense_tour.gif", save_all=True, append_images=pal[1:], duration=1600, loop=0, optimize=True)
        for f in frames:
            f.unlink()

        # ---------------------------------------------------------- screen-recording (webm)
        if video:
            ctx = await browser.new_context(viewport={"width": 1440, "height": 900},
                                            record_video_dir=str(DEMO), record_video_size={"width": 1440, "height": 900})
            page = await ctx.new_page()
            await page.goto(base)
            await page.evaluate("localStorage.setItem('ss-theme', 'dark')")
            for route, _ in PAGES:
                await page.goto(f"{base}/#/{route.format(cid=cid)}")
                await page.wait_for_timeout(2000)
                for y in range(0, 2200, 350):
                    await page.mouse.wheel(0, 350)
                    await page.wait_for_timeout(350)
                if route == "churn":            # interact with the what-if simulator
                    await page.evaluate("window.scrollTo(0, 0)")
                    sl = page.locator('#sim input[type=range]').first
                    await sl.scroll_into_view_if_needed()
                    box = await sl.bounding_box()
                    await page.mouse.move(box["x"] + 5, box["y"] + box["height"] / 2)
                    await page.mouse.down()
                    for step in range(1, 25):
                        await page.mouse.move(box["x"] + step * box["width"] / 24, box["y"] + box["height"] / 2)
                        await page.wait_for_timeout(60)
                    await page.mouse.up()
                    await page.wait_for_timeout(1200)
            vid = page.video
            await ctx.close()
            src = Path(await vid.path())
            target = DEMO / "shopsense_demo.webm"
            if target.exists():
                target.unlink()
            src.rename(target)
        await browser.close()
    print("screenshots ->", SHOTS, "\ndemo        ->", DEMO)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--customer", default="C000002")
    ap.add_argument("--no-video", action="store_true")
    a = ap.parse_args()
    asyncio.run(main(a.base, a.customer, not a.no_video))
