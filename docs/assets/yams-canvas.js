/* YAMS Full-Site Living Substrate ("Quiet Paper, Living Substrate")
 * No boxes, no visible widgets: the entire site background is a calm archival
 * paper / silicon substrate that subtly responds to pointer movement, scrolling,
 * and sketching on open whitespace — without ever blocking text or links.
 */
(function () {
  'use strict';

  function fnv1aHex(str) {
    let h1 = 0x811c9dc5;
    let h2 = 0x01000193;
    for (let i = 0; i < str.length; i++) {
      const c = str.charCodeAt(i);
      h1 ^= c;
      h1 = Math.imul(h1, 0x01000193);
      h2 ^= (c + i) & 0xff;
      h2 = Math.imul(h2, 0x85ebca6b);
    }
    const a = (h1 >>> 0).toString(16).padStart(8, '0');
    const b = (h2 >>> 0).toString(16).padStart(8, '0');
    return (a + b).slice(0, 8);
  }

  function getPalette() {
    const scheme = document.body.getAttribute('data-md-color-scheme') || 'default';
    if (scheme === 'slate') {
      return {
        dark: true,
        bg: '#0e1117',
        grid: 'rgba(223, 177, 71, 0.055)',
        ink: '#dfb147',
        inkSoft: 'rgba(223, 177, 71, 0.22)',
        terracotta: '#d97a3e',
        leaf: '#6bc458',
        muted: 'rgba(210, 216, 224, 0.45)',
        edge: 'rgba(217, 122, 62, 0.14)',
        pillBg: 'rgba(18, 22, 32, 0.90)',
        pillBorder: 'rgba(223, 177, 71, 0.30)',
      };
    }
    return {
      dark: false,
      bg: '#fbf9f4',
      grid: 'rgba(59, 91, 117, 0.065)',
      ink: '#c86d3b',
      inkSoft: 'rgba(200, 109, 59, 0.20)',
      terracotta: '#c86d3b',
      leaf: '#4d923c',
      muted: 'rgba(43, 46, 48, 0.42)',
      edge: 'rgba(59, 91, 117, 0.12)',
      pillBg: 'rgba(251, 249, 244, 0.94)',
      pillBorder: 'rgba(59, 91, 117, 0.24)',
    };
  }

  function isInteractiveTarget(target) {
    if (!target || !(target instanceof Element)) return false;
    return Boolean(
      target.closest(
        'a, button, input, textarea, select, code, pre, label, summary, .md-header, .md-tabs, .md-search, .md-clipboard'
      )
    );
  }

  function mountFullSiteCanvas() {
    if (document.getElementById('yams-substrate-canvas')) return;

    const canvas = document.createElement('canvas');
    canvas.id = 'yams-substrate-canvas';
    canvas.setAttribute('aria-hidden', 'true');
    document.body.appendChild(canvas);

    const ctx = canvas.getContext('2d');
    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    let palette = getPalette();

    let width = window.innerWidth;
    let height = window.innerHeight;
    let dpr = Math.min(window.devicePixelRatio || 1, 2);
    let rafId = null;
    let lastActivity = performance.now();
    let scrollY = window.scrollY || 0;

    const nodes = [];
    const strokes = [];
    let drawingStroke = null;
    let pointer = { x: -999, y: -999, inside: false };
    let hoveredNode = null;

    function seedMarginNodes() {
      nodes.length = 0;
      const seeds = [
        { rx: 0.06, ry: 0.22, label: 'cas.idx', bytes: 1420, ratio: 3.1 },
        { rx: 0.93, ry: 0.26, label: 'paper.pdf', bytes: 48200, ratio: 2.8 },
        { rx: 0.08, ry: 0.68, label: 'notes.md', bytes: 3180, ratio: 3.6 },
        { rx: 0.92, ry: 0.72, label: 'graph.db', bytes: 9600, ratio: 4.2 },
      ];
      seeds.forEach((s) => {
        nodes.push({
          x: s.rx * width,
          y: s.ry * height,
          rx: s.rx,
          ry: s.ry,
          label: s.label,
          hash: fnv1aHex(s.label),
          bytes: s.bytes,
          ratio: s.ratio,
          pulse: 0,
          isSeed: true,
        });
      });
    }

    function resize() {
      width = window.innerWidth;
      height = window.innerHeight;
      dpr = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.floor(width * dpr);
      canvas.height = Math.floor(height * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

      if (nodes.length === 0) {
        seedMarginNodes();
      } else {
        nodes.forEach((n) => {
          n.x = Math.max(18, Math.min(width - 18, n.rx * width));
          n.y = Math.max(64, Math.min(height - 24, n.ry * height));
        });
      }
      wake();
    }

    function wake() {
      lastActivity = performance.now();
      if (!rafId) {
        rafId = requestAnimationFrame(tick);
      }
    }

    function addStrokeNode(pts) {
      let sx = 0;
      let sy = 0;
      pts.forEach((p) => {
        sx += p.x;
        sy += p.y;
      });
      const cx = sx / pts.length;
      const cy = sy / pts.length;
      const key = 'ink:' + Math.round(cx) + ':' + Math.round(cy) + ':' + nodes.length;
      const hash = fnv1aHex(key);
      const rawBytes = Math.max(320, pts.length * 44);
      const ratio = Number((2.5 + ((parseInt(hash.slice(0, 2), 16) % 18) / 10)).toFixed(1));

      nodes.push({
        x: cx,
        y: cy,
        rx: cx / width,
        ry: cy / height,
        label: 'sha256:' + hash.slice(0, 6),
        hash: hash,
        bytes: rawBytes,
        ratio: ratio,
        pulse: 1,
        isSeed: false,
      });

      strokes.push({ points: pts, alpha: 0.58 });
      if (strokes.length > 10) strokes.shift();
      if (nodes.length > 14) {
        const idx = nodes.findIndex((n) => !n.isSeed);
        if (idx >= 0) nodes.splice(idx, 1);
      }
      wake();
    }

    window.addEventListener(
      'pointermove',
      (e) => {
        pointer = { x: e.clientX, y: e.clientY, inside: true };
        if (drawingStroke) {
          const last = drawingStroke[drawingStroke.length - 1];
          if (Math.hypot(e.clientX - last.x, e.clientY - last.y) > 5) {
            drawingStroke.push({ x: e.clientX, y: e.clientY });
          }
        }
        hoveredNode = null;
        let best = 24;
        for (const n of nodes) {
          const d = Math.hypot(e.clientX - n.x, e.clientY - n.y);
          if (d < best) {
            best = d;
            hoveredNode = n;
          }
        }
        wake();
      },
      { passive: true }
    );

    // Allow sketching on open whitespace/margins without ever hijacking text selection.
    window.addEventListener('pointerdown', (e) => {
      if (e.button !== 0 || reducedMotion) return;
      if (isInteractiveTarget(e.target)) return;
      // Only start a sketch if clicking outside dense prose text or in the margins.
      const sel = window.getSelection();
      if (sel && !sel.isCollapsed) return;
      const target = e.target;
      const isBlankPaper =
        target === document.body ||
        (target instanceof Element &&
          (target.classList.contains('md-main') ||
            target.classList.contains('md-main__inner') ||
            target.classList.contains('md-grid') ||
            target.classList.contains('md-content')));
      if (!isBlankPaper) return;

      drawingStroke = [{ x: e.clientX, y: e.clientY }];
      wake();
    });

    function finishStroke() {
      if (!drawingStroke) return;
      const pts = drawingStroke;
      drawingStroke = null;
      if (pts.length >= 3) {
        addStrokeNode(pts);
      }
    }

    window.addEventListener('pointerup', finishStroke, { passive: true });
    window.addEventListener('pointercancel', finishStroke, { passive: true });
    window.addEventListener(
      'scroll',
      () => {
        scrollY = window.scrollY || 0;
        wake();
      },
      { passive: true }
    );

    function draw() {
      ctx.clearRect(0, 0, width, height);

      // 1. Subtle parallax archival dot-grid across the whole viewport.
      const step = 32;
      const offsetY = -(scrollY * 0.14) % step;
      ctx.fillStyle = palette.grid;
      for (let x = step / 2; x < width; x += step) {
        for (let y = offsetY; y < height; y += step) {
          ctx.fillRect(x, y, 1.4, 1.4);
        }
      }

      // 2. Settled user ink strokes (quietly fade into archival trace).
      for (const s of strokes) {
        if (s.points.length < 2) continue;
        ctx.beginPath();
        ctx.moveTo(s.points[0].x, s.points[0].y);
        for (let i = 1; i < s.points.length; i++) {
          ctx.lineTo(s.points[i].x, s.points[i].y);
        }
        ctx.strokeStyle = palette.terracotta;
        ctx.globalAlpha = s.alpha;
        ctx.lineWidth = 1.8;
        ctx.lineCap = 'round';
        ctx.lineJoin = 'round';
        ctx.stroke();
        ctx.globalAlpha = 1;
        if (!reducedMotion && s.alpha > 0.12) {
          s.alpha *= 0.992;
        }
      }

      if (drawingStroke && drawingStroke.length > 1) {
        ctx.beginPath();
        ctx.moveTo(drawingStroke[0].x, drawingStroke[0].y);
        for (let i = 1; i < drawingStroke.length; i++) {
          ctx.lineTo(drawingStroke[i].x, drawingStroke[i].y);
        }
        ctx.strokeStyle = palette.ink;
        ctx.lineWidth = 2.2;
        ctx.lineCap = 'round';
        ctx.lineJoin = 'round';
        ctx.stroke();
      }

      // 3. Delicate root edges between nearby nodes & pointer.
      for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
          const a = nodes[i];
          const b = nodes[j];
          const dist = Math.hypot(a.x - b.x, a.y - b.y);
          if (dist > 260) continue;
          const mx = (a.x + b.x) / 2;
          const my = (a.y + b.y) / 2 - Math.min(18, dist * 0.08);
          ctx.beginPath();
          ctx.moveTo(a.x, a.y);
          ctx.quadraticCurveTo(mx, my, b.x, b.y);
          ctx.strokeStyle = palette.edge;
          ctx.globalAlpha = Math.max(0.06, 1 - dist / 260);
          ctx.lineWidth = 1;
          ctx.stroke();
          ctx.globalAlpha = 1;
        }
      }

      if (pointer.inside) {
        for (const n of nodes) {
          const dist = Math.hypot(pointer.x - n.x, pointer.y - n.y);
          if (dist < 170) {
            ctx.beginPath();
            ctx.moveTo(n.x, n.y);
            ctx.lineTo(pointer.x, pointer.y);
            ctx.strokeStyle = palette.inkSoft;
            ctx.globalAlpha = (1 - dist / 170) * 0.8;
            ctx.lineWidth = 1;
            ctx.stroke();
            ctx.globalAlpha = 1;
          }
        }
      }

      // 4. Quiet margin nodes + hover technical pill.
      ctx.font = '500 10.5px "JetBrains Mono", ui-monospace, SFMono-Regular, monospace';
      for (const n of nodes) {
        if (!reducedMotion && n.pulse > 0.01) {
          n.pulse *= 0.92;
        }
        const isHover = hoveredNode === n;
        if (n.pulse > 0.04) {
          ctx.beginPath();
          ctx.arc(n.x, n.y, 6 + (1 - n.pulse) * 20, 0, Math.PI * 2);
          ctx.strokeStyle = palette.leaf;
          ctx.globalAlpha = n.pulse * 0.5;
          ctx.lineWidth = 1.2;
          ctx.stroke();
          ctx.globalAlpha = 1;
        }

        ctx.beginPath();
        ctx.arc(n.x, n.y, isHover ? 5.5 : 3.8, 0, Math.PI * 2);
        ctx.fillStyle = n.isSeed ? palette.terracotta : palette.leaf;
        ctx.globalAlpha = isHover ? 0.9 : 0.45;
        ctx.fill();
        ctx.globalAlpha = 1;

        if (isHover || n.pulse > 0.25) {
          const meta = 'sha256:' + n.hash + ' · ' + n.bytes + ' B · ' + n.ratio + '×';
          const padX = 7;
          const boxW = ctx.measureText(meta).width + padX * 2;
          const boxH = 20;
          const bx = Math.min(width - boxW - 10, Math.max(10, n.x - boxW / 2));
          const by = Math.max(64, n.y - 30);

          ctx.fillStyle = palette.pillBg;
          ctx.strokeStyle = palette.pillBorder;
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.roundRect(bx, by, boxW, boxH, 6);
          ctx.fill();
          ctx.stroke();

          ctx.fillStyle = palette.ink;
          ctx.fillText(meta, bx + padX, by + 13.5);
        }
      }
    }

    function tick(now) {
      draw();
      const active =
        drawingStroke ||
        pointer.inside ||
        now - lastActivity < 1400;

      if (active && !reducedMotion) {
        rafId = requestAnimationFrame(tick);
      } else {
        rafId = null;
      }
    }

    const observer = new MutationObserver(() => {
      palette = getPalette();
      wake();
    });
    observer.observe(document.body, {
      attributes: true,
      attributeFilter: ['data-md-color-scheme'],
    });

    window.addEventListener('resize', resize);
    resize();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mountFullSiteCanvas);
  } else {
    mountFullSiteCanvas();
  }
})();
