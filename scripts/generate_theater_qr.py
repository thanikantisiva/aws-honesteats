"""
Generate a deep-link QR code for theater (in-venue) ordering.

The QR encodes ``rork-app://theater/{restaurantId}``. When a customer scans it
inside the rork-honesteats customer app, the deep-link handler routes them
straight into the brand-new theater flow (seat capture → curated menu →
isolated cart → Razorpay-only checkout → pickup token).

The output PNG includes the venue name printed below the QR so operators can
print/laminate it without having to label each copy manually.

Usage examples
--------------

# Minimal — writes ./theater_qr_RES-1234.png
python scripts/generate_theater_qr.py --restaurant-id RES-1776878473046-8935

# Custom venue label + output path + larger QR
python scripts/generate_theater_qr.py \\
    --restaurant-id RES-1776878473046-8935 \\
    --venue-name "PVR Forum Mall — Screen 3" \\
    --output ./out/pvr_forum_screen3.png \\
    --box-size 14

Requirements:
    pip install "qrcode[pil]"
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

try:
    import qrcode
    from PIL import Image, ImageDraw, ImageFont
except ImportError as exc:  # pragma: no cover - import-time guard
    print(
        "Missing dependency. Install with:\n"
        '    pip install "qrcode[pil]"',
        file=sys.stderr,
    )
    raise SystemExit(1) from exc


DEEPLINK_SCHEME = "rork-app://theater/"
DEFAULT_BOX_SIZE = 12
DEFAULT_BORDER = 4
LABEL_PADDING = 20
LABEL_GAP = 12


def _build_payload(restaurant_id: str, web_base_url: Optional[str] = None) -> str:
    """Compose the payload that gets encoded in the QR.

    - When ``web_base_url`` is provided (e.g. ``https://yumdude.com``), the QR
      encodes ``{web_base_url}/linktree/{restaurant_id}``. That URL is served
      by the Linktree landing page in the restaurant POV Angular app
      (``src/app/features/linktree``) which attempts the
      ``rork-app://theater/{id}`` deep link first and falls back to the
      App Store / Play Store if the app isn't installed. Once Universal
      Links / App Links are configured for the domain, the OS will open the
      customer app silently using the same URL — no need to reprint QRs.
    - When omitted, the QR encodes the raw ``rork-app://theater/{id}`` custom
      scheme. Useful for in-app scanners (Rork app already handles this) but
      cannot do "download if missing".
    """
    rid = restaurant_id.strip()
    if not rid:
        raise ValueError("restaurant_id must be a non-empty string")
    if web_base_url:
        base = web_base_url.strip().rstrip("/")
        if not (base.startswith("http://") or base.startswith("https://")):
            raise ValueError("--web-base-url must start with http:// or https://")
        return f"{base}/linktree/{rid}"
    return f"{DEEPLINK_SCHEME}{rid}"


def _load_font(preferred_size: int) -> ImageFont.ImageFont:
    """Try a couple of common system fonts; fall back to PIL default."""
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        "/System/Library/Fonts/SFNS.ttf",
        "/Library/Fonts/Arial Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, preferred_size)
            except OSError:
                continue
    return ImageFont.load_default()


def _render_label(
    qr_img: Image.Image,
    venue_name: str,
    restaurant_id: str,
    font_size: int,
) -> Image.Image:
    """Add venue + restaurant-id captions below the QR for easy identification."""
    label_font = _load_font(font_size)
    sub_font = _load_font(max(11, font_size - 6))

    # Measure both lines so the canvas is just the right height.
    dummy = Image.new("RGB", (1, 1))
    draw = ImageDraw.Draw(dummy)
    line1 = venue_name.strip() or restaurant_id
    line2 = f"Scan with the Rork app · {restaurant_id}"

    bbox1 = draw.textbbox((0, 0), line1, font=label_font)
    bbox2 = draw.textbbox((0, 0), line2, font=sub_font)
    text_w = max(bbox1[2] - bbox1[0], bbox2[2] - bbox2[0])
    text_h = (bbox1[3] - bbox1[1]) + LABEL_GAP + (bbox2[3] - bbox2[1])

    canvas_w = max(qr_img.size[0], text_w + LABEL_PADDING * 2)
    canvas_h = qr_img.size[1] + LABEL_PADDING + text_h + LABEL_PADDING

    canvas = Image.new("RGB", (canvas_w, canvas_h), "white")
    qr_x = (canvas_w - qr_img.size[0]) // 2
    canvas.paste(qr_img, (qr_x, 0))

    draw = ImageDraw.Draw(canvas)
    text_x = (canvas_w - (bbox1[2] - bbox1[0])) // 2
    text_y = qr_img.size[1] + LABEL_PADDING
    draw.text((text_x, text_y), line1, fill="black", font=label_font)

    sub_x = (canvas_w - (bbox2[2] - bbox2[0])) // 2
    sub_y = text_y + (bbox1[3] - bbox1[1]) + LABEL_GAP
    draw.text((sub_x, sub_y), line2, fill="#555555", font=sub_font)

    return canvas


def generate_qr(
    restaurant_id: str,
    output_path: str,
    venue_name: Optional[str] = None,
    box_size: int = DEFAULT_BOX_SIZE,
    border: int = DEFAULT_BORDER,
    label_font_size: int = 22,
    web_base_url: Optional[str] = None,
) -> str:
    """Render a theater QR PNG and return the absolute output path."""
    payload = _build_payload(restaurant_id, web_base_url=web_base_url)

    qr = qrcode.QRCode(
        version=None,  # auto-pick smallest fit
        error_correction=qrcode.constants.ERROR_CORRECT_H,  # robust to printing artifacts
        box_size=box_size,
        border=border,
    )
    qr.add_data(payload)
    qr.make(fit=True)

    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")

    final = _render_label(qr_img, venue_name or "", restaurant_id, label_font_size)

    out_dir = os.path.dirname(os.path.abspath(output_path))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    final.save(output_path, format="PNG")
    abs_path = os.path.abspath(output_path)
    print(f"Wrote {abs_path}")
    print(f"Encoded payload: {payload}")
    return abs_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a theater-mode deep-link QR code (rork-app://theater/{restaurantId})",
    )
    parser.add_argument(
        "--restaurant-id",
        required=True,
        help="Restaurant ID (e.g. RES-1776878473046-8935). The 'RESTAURANT#' prefix is NOT included.",
    )
    parser.add_argument(
        "--venue-name",
        default="",
        help="Optional venue display name to print below the QR (e.g. 'PVR Forum Mall — Screen 3').",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output PNG path. Defaults to ./theater_qr_<restaurantId>.png",
    )
    parser.add_argument(
        "--box-size",
        type=int,
        default=DEFAULT_BOX_SIZE,
        help=f"Pixel size per QR module. Default: {DEFAULT_BOX_SIZE}.",
    )
    parser.add_argument(
        "--border",
        type=int,
        default=DEFAULT_BORDER,
        help=f"QR quiet-zone border (in modules). Default: {DEFAULT_BORDER}.",
    )
    parser.add_argument(
        "--label-font-size",
        type=int,
        default=22,
        help="Font size for the venue label printed below the QR.",
    )
    parser.add_argument(
        "--web-base-url",
        default=None,
        help=(
            "Optional. When set (e.g. 'https://yumdude.com'), the QR encodes "
            "'{base}/linktree/{restaurantId}' instead of the raw rork-app:// "
            "scheme. The Linktree route in the restaurant POV Angular app "
            "handles the deep-link attempt and store fallback, giving the "
            "single QR dual-purpose 'open app or download from store' "
            "behaviour."
        ),
    )
    args = parser.parse_args()

    rid = args.restaurant_id.strip()
    if rid.startswith("RESTAURANT#"):
        rid = rid.split("#", 1)[1]
        print(
            "Note: stripped 'RESTAURANT#' prefix; "
            f"using restaurantId='{rid}' in the deep link.",
        )

    output_path = args.output or f"theater_qr_{rid}.png"

    generate_qr(
        restaurant_id=rid,
        output_path=output_path,
        venue_name=args.venue_name,
        box_size=args.box_size,
        border=args.border,
        label_font_size=args.label_font_size,
        web_base_url=args.web_base_url,
    )


if __name__ == "__main__":
    main()
