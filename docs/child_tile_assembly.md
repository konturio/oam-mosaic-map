# Controlling Child Tile Assembly and Recursion

## Overview

This change introduces configuration to control or disable assembling a parent tile from its four children ("child assembly"). The goal is to reduce CPU load and request fan-out at low zoom levels, which can otherwise cause large queues in the raster processing pipeline.

## What Was Changed

1. Child assembly gating by zoom
   - New env var: `CHILD_ASSEMBLY_MAX_ZOOM`.
   - Behavior:
     - If `CHILD_ASSEMBLY_MAX_ZOOM = 0` (default), child assembly is fully disabled.
     - If `CHILD_ASSEMBLY_MAX_ZOOM > 0`, assembly is allowed only for zooms `z < CHILD_ASSEMBLY_MAX_ZOOM`.

2. Recursion depth limit
   - New env var: `CHILD_ASSEMBLY_MAX_DEPTH`.
   - Behavior:
     - If `CHILD_ASSEMBLY_MAX_DEPTH = 0` (default), child assembly is fully disabled.
     - If `CHILD_ASSEMBLY_MAX_DEPTH > 0`, recursion is allowed only while `depth < CHILD_ASSEMBLY_MAX_DEPTH`.

3. Integration points
   - Per-image tile path (`source(...)` in `src/mosaic.mjs`):
     - Child assembly path executes only when both zoom and depth constraints are satisfied and `z < meta.maxzoom`.
   - Mosaic path (`buildRowTilePromises(...)` in `src/mosaic.mjs`):
     - The optional `parentTilePromise` (assemble parent from 4 children) is created only if both constraints are satisfied.

4. Robustness fixes
   - `Tile.extractChild(...)` (in `src/tile.mjs`) now always returns a `Tile` (returns an empty `Tile` when the image is empty) to avoid ambiguous return types.
   - `mosaic256px(...)` uses `await tile.transformInJpegIfFullyOpaque()` to avoid accessing internals directly.

## Rationale

- Child assembly can trigger an exponential fan-out of work at low zoom levels, especially with a cold cache.
- Limiting by zoom and depth reduces CPU usage, TiTiler traffic, and queue buildup while keeping the behavior configurable per environment.

## Configuration

```bash
# Disable child assembly entirely (default)
export CHILD_ASSEMBLY_MAX_ZOOM=0
export CHILD_ASSEMBLY_MAX_DEPTH=0

# Enable assembly below z=9 with up to 2 levels of recursion
export CHILD_ASSEMBLY_MAX_ZOOM=9
export CHILD_ASSEMBLY_MAX_DEPTH=2

# Enable assembly broadly (use with caution)
export CHILD_ASSEMBLY_MAX_ZOOM=24
export CHILD_ASSEMBLY_MAX_DEPTH=8
```

Notes:
- Both constraints must be satisfied for assembly to run.
- If either value is `0` (or non-numeric), assembly is disabled.

## Impact

- Lower CPU and fewer concurrent requests at low zoom levels.
- Potentially less detail at very low zooms if parent tiles previously relied on assembling from children; this is controllable via the env settings above.
- No changes to outlines/clusters endpoints; only the mosaic tile pipeline is affected.

## Files Touched

- `src/mosaic.mjs`:
  - Added `CHILD_ASSEMBLY_MAX_ZOOM` and `CHILD_ASSEMBLY_MAX_DEPTH` gates.
  - Plumbed recursion depth through `source(...)`, `mosaic512px(...)`, and `buildRowTilePromises(...)`.
  - Adjusted JPEG conversion call site.
- `src/tile.mjs`:
  - `Tile.extractChild(...)` now returns a `Tile` in all cases.


