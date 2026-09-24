#!/usr/bin/env python3
"""TrOCR handwriting recogniser for remarkable-server.

Usage (server side):
    HWR_COMMAND="python3 /path/to/contrib/hwr/trocr_hwr.py" remarkable-server ...

Reads a PNG of rendered ink on stdin and prints Tesseract-format TSV (level-5 word rows)
on stdout, which is what src/handwriting.rs parses.

Tesseract is still used, only for layout: it finds the text lines. Each line is then read
by TrOCR (a transformer trained on handwriting), which does much better than Tesseract on
cursive. TrOCR returns text per line without word positions, so word boxes are estimated
by splitting the line box in proportion to word length.

Requires: tesseract, pillow, torch, transformers.
    pip install pillow torch transformers
Model: $TROCR_MODEL (default microsoft/trocr-base-handwritten), downloaded on first use.
"""
import io
import os
import subprocess
import sys

from PIL import Image

MODEL = os.environ.get("TROCR_MODEL", "microsoft/trocr-base-handwritten")
LANG = os.environ.get("HWR_LANG", "eng")
HEADER = "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext"


def tesseract_lines(png):
    """Line boxes (block, par, line, left, top, width, height) from Tesseract's layout pass."""
    out = subprocess.run(
        ["tesseract", "stdin", "stdout", "-l", LANG, "--psm", "6", "tsv"],
        input=png, capture_output=True, check=True,
    ).stdout.decode("utf-8", "replace")
    lines = []
    for row in out.splitlines()[1:]:
        f = row.split("\t")
        if len(f) >= 12 and f[0] == "4":
            lines.append(tuple(int(x) for x in (f[2], f[3], f[4], f[6], f[7], f[8], f[9])))
    return lines


def main():
    png = sys.stdin.buffer.read()
    image = Image.open(io.BytesIO(png)).convert("RGB")
    lines = tesseract_lines(png)

    rows = [HEADER]
    if lines:
        # Imported here so a page with no ink never pays the model load.
        from transformers import TrOCRProcessor, VisionEncoderDecoderModel

        processor = TrOCRProcessor.from_pretrained(MODEL)
        model = VisionEncoderDecoderModel.from_pretrained(MODEL)
        pad = 8
        for block, par, line, left, top, width, height in lines:
            crop = image.crop((max(0, left - pad), max(0, top - pad),
                               min(image.width, left + width + pad), min(image.height, top + height + pad)))
            pixels = processor(images=crop, return_tensors="pt").pixel_values
            text = processor.batch_decode(model.generate(pixels, max_new_tokens=64),
                                          skip_special_tokens=True)[0].strip()
            words = text.split()
            total = sum(len(w) for w in words) + max(0, len(words) - 1)
            x = float(left)
            for i, word in enumerate(words, 1):
                w = width * len(word) / total if total else width
                rows.append("\t".join(str(v) for v in (
                    5, 1, block, par, line, i, round(x), top, max(1, round(w)), height, 90, word)))
                x += w + width / total if total else w
    sys.stdout.write("\n".join(rows) + "\n")


if __name__ == "__main__":
    main()
