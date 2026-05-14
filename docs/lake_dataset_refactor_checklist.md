# LakeDataset Refactor Checklist

## Goal

Replace the single-lake `LakeTask` execution model with a worker-slice `LakeDataset` model:

- provider handles IO only
- dataset factory builds the final in-memory worker slice
- calculators consume dataset batches rather than per-lake tasks
- workers no longer assemble ad hoc `dict[int, DataFrame]` payloads

## Milestones

### Milestone 1: Dataset Foundation

- [x] create isolated worktree and feature branch
- [x] add `LakeDataset` data container
- [x] add `LakeDatasetQuery` query/spec object
- [x] add `LakeDatasetFactory` with provider-backed build path
- [x] add unit tests for quality/range/subset/done filtering and array assembly
- [ ] self-review milestone diff
- [ ] commit milestone 1

### Milestone 2: Single-Process Dataset Runner

- [ ] add dataset-backed single-process runner
- [ ] keep legacy single-process runner intact during transition
- [ ] adapt one primary calculator (`pwm_extreme`) to batch dataset input
- [ ] run focused single-process tests
- [ ] self-review milestone diff
- [ ] commit milestone 2

### Milestone 3: Calculator Batch Interface

- [ ] change batch calculator interface from `run(task)` to dataset batch entrypoint
- [ ] migrate `quantile`
- [ ] migrate `pwm_extreme`
- [ ] migrate `eot`
- [ ] keep output row parity with legacy path
- [ ] run focused calculator tests
- [ ] self-review milestone diff
- [ ] commit milestone 3

### Milestone 4: MPI Worker Slice Path

- [ ] refactor worker path to build `LakeDataset` slices directly
- [ ] preserve manager scheduling and row aggregation behavior
- [ ] validate done-id exclusion still works under MPI
- [ ] run MPI smoke/focused protocol tests
- [ ] self-review milestone diff
- [ ] commit milestone 4

### Milestone 5: Script Migration And Cleanup

- [ ] migrate `run_pwm_extreme.py`
- [ ] migrate `run_quantile.py`
- [ ] migrate remaining batch scripts incrementally
- [ ] mark legacy `Dataset`/`LakeTask` path for removal or keep as compatibility shim if still needed
- [ ] run focused regression suite
- [ ] write AI review report in `docs/reviews/`
- [ ] final self-review, test, and merge

## Constraints

- prefer worker-slice construction over full-dataset materialization
- keep changes minimal per milestone
- do not change algorithm math in the early milestones
- preserve existing storage row formats unless explicitly required later
