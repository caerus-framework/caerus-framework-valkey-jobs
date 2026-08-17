# caerus-framework-valkey-jobs

ARCHIVED. DO NOT USE

[![CI](https://github.com/caerus-framework/caerus-framework-valkey-jobs/actions/workflows/ci.yml/badge.svg)](https://github.com/caerus-framework/caerus-framework-valkey-jobs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/caerus-framework/caerus-framework-valkey-jobs/graph/badge.svg)](https://codecov.io/gh/caerus-framework/caerus-framework-valkey-jobs)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Caerus Framework Valkey Jobs Component. A lightweight **delayed task queue**
built on valkey sorted sets and Lua: enqueue a job with an optional `run-at`
time, a worker claims due jobs and runs registered handlers, failed jobs retry
with a mixed fixed/jittered-exponential policy, and jobs that exhaust their
attempts (or have no handler) go to a dead-letter set for inspection.

Delivery is **at-least-once**: a handler may run more than once (crash between
claim and ack, or a visibility-timeout recovery), so handlers must be
idempotent. There is no cross-job ordering or exactly-once guarantee.

## Wiring

Two wiring shapes are supported. Prefer the **app-owned** shape (demoapp
golden path): `main` declares only the chassis (valkey-jobs alongside postgres /
valkey) and the app class; product machinery that produces/consumes jobs lives
under the app and resolves valkey-jobs as a peer at `Init`. Use the simple
`main`-level shape for one-off binaries.

### App-owned consumer (golden — demoapp pattern)

`main` declares valkey-jobs as chassis and runs the app class; it never touches
valkey-jobs itself:

```go
fw := cf.New(&cf.FrameworkOptions{
	Logs: &cf.LogsSettings{Format: "json", Level: "info", ConfigSource: "logs"},
	Observability: &cf.ObservabilitySettings{Bind: ":9090", ConfigSource: "observability"},
	Components: []cf.CaerusComponent{
		cf_valkey.New(cf_valkey.WithConfigSource("valkey", "config/valkey.json")),
		cf_valkey_jobs.New(cf_valkey_jobs.WithConfigSource("jobs", "config/jobs.json")),
		app.New(app.Options{}),
	},
})
if err := fw.RunWithSignals(context.Background()); err != nil {
	log.Fatal(err)
}
```

The app resolves the valkey-jobs **component pointer** once at `Init` (never a
client snapshot), declares it in `GetDependencies`, and calls `Enqueue` /
registers handlers per use:

```go
type App struct {
	jobs *cf_valkey_jobs.CFValkeyJobs
}

func (a *App) GetDependencies() []string {
	return []string{cf_valkey_jobs.ComponentName} // + logs, chassis peers
}

func (a *App) Init(ctx context.Context, fw *cf.CaerusFramework) error {
	j, ok := cf.Get[*cf_valkey_jobs.CFValkeyJobs](fw)
	if !ok {
		return errors.New("app: jobs component missing")
	}
	a.jobs = j
	return nil
}
```

### Simple `main`-level wiring

For a one-off binary, register the components directly and use
`cf.MustGet` to reach the component:

```go
fw := cf.New()

logs := cf_logs.New(cf_logs.WithWriter(os.Stdout))
vk := cf_valkey.New(cf_valkey.WithConfigSource("valkey", "config/valkey.json"))
jobs := cf_valkey_jobs.New(cf_valkey_jobs.WithConfigSource("jobs", "config/jobs.json"))
fw.AddComponent(logs)
fw.AddComponent(vk)
fw.AddComponent(jobs) // GetDependencies() -> [valkey logs configuration]
```

In both shapes the component is `cf.ConfigSourceRegistrar`-self-sufficient:
`WithConfigSource` registers the `Source[JobsConfig]` with the configuration
component during argv absorption, so `main` never touches
`os.Getenv`/`ParseFlags`. The `--jobs` path flag and per-field flags come from
the source declaration.

## Usage

Register handlers before the framework starts. The worker is only enabled when
at least one handler is registered and `worker_enabled` is true (default).

```go
jobs := cf_valkey_jobs.New(
	cf_valkey_jobs.WithConfigSource("jobs", "config/jobs.json"),
	cf_valkey_jobs.WithJobHandler("email.send", func(ctx context.Context, job cf_valkey_jobs.Job) error {
		// job.Payload is the raw bytes; return an error to retry.
		return sendEmail(ctx, job.Payload)
	}),
)

// elsewhere, via the app's resolved peer
id, err := app.jobs.Enqueue(ctx, "email.send", []byte(`{"to":"a@b.c"}`),
	cf_valkey_jobs.WithDelay(5*time.Minute),
	cf_valkey_jobs.WithMaxAttempts(5),
)
```

`Enqueue` returns the job id. Scheduling options:

| Option | Description |
| --- | --- |
| `WithRunAt(t)` | claimable from `t` (zero/past = immediately) |
| `WithDelay(d)` | run `d` from now (overrides `WithRunAt`) |
| `WithMaxAttempts(n)` | max runs before dead-letter (default `3`) |
| `WithVisibility(d)` | time a claimed job stays hidden before recovery (default `1m`) |
| `WithRetention(d)` | TTL on the job payload (default `7d`) |

## Retry policy

On a handler error the job is requeued with a **mixed** policy:

1. While `now − first-failure ≤ retry_fixed_phase` (default 30s), retry every
   `retry_fixed_delay` (default 5s).
2. Past the fixed phase, back off exponentially counting attempts since the
   phase switch: `retry_fixed_delay · 2^(n−1)` for `n = attempts − jitter_base`,
   jittered ±`retry_jitter` (default 50%), capped at `retry_max_delay`
   (default 5m).

When a job exhausts `max_attempts` (or no handler is registered for its type) it
is dead-lettered into the `jobs:dead` zset for inspection; the payload hash
survives until retention expires. Failed runs increment `attempts` in the
payload hash, so `Job.Attempts` seen by a handler is 1-based.

## Options

| Option | Description |
| --- | --- |
| `WithConfig(JobsConfig)` | static config snapshot; non-zero fields override option-set defaults |
| `WithConfigSource(name, path, …)` | bind a configuration source for Init + `OnConfigReload`; the module registers the `Source[JobsConfig]` itself (declares `configuration` dep) |
| `WithJobHandler(type, fn)` | register a handler for a job type |
| `WithWorkerEnabled(on)` | force the worker on/off (default on once handlers exist) |
| `WithPollInterval(d)` | worker poll cadence (default `500ms`) |
| `WithBatchSize(n)` | max jobs claimed per pass (default `16`) |
| `WithConcurrency(n)` | max concurrent handler runs (default `8`) |
| `WithRetryPolicy(fixed, phase, max)` | retry tunables (defaults 5s / 30s / 5m) |
| `WithShutdownDrainTimeout(d)` | max time to wait for running handlers on shutdown (default `10s`) |
| `WithValkeyName(name)` | name of the valkey peer to use (default `"valkey"`) |
| `WithName(name)` | custom component name for multiple instances (default `"valkey-jobs"`) |
| `WithLogger(*slog.Logger)` | explicit logger override; defaults to the framework `logs` component's logger (re-delivered on `logs` `Reconfigure`), falling back to `slog.Default()` |

## Configuration

Load `JobsConfig` through the configuration component. The default
`EnvPrefix` is `JOBS_` (from the source name). Worker tunables reload live via
`OnConfigReload`; the retry math and visibility are read fresh per decision.

```json
{
  "worker_enabled": true,
  "poll_interval_ms": 500,
  "batch_size": 16,
  "concurrency": 8,
  "retry_fixed_delay_ms": 5000,
  "retry_fixed_phase_ms": 30000,
  "retry_max_delay_ms": 300000,
  "retry_jitter": 0.5
}
```

`Health` reports healthy once the valkey peer is initialized (client present);
before Init it reports unhealthy. `Metrics` emits the following while
initialized, nil before Init/after Shutdown:

| Metric | Type | Labels |
| --- | --- | --- |
| `valkey_jobs_info` | gauge 1 | `component` |
| `valkey_jobs_config_reloads_total` | counter | `component` |
| `valkey_jobs_enqueued_total` | counter | `component`, `type` |
| `valkey_jobs_run_total` | counter | `component`, `type` |
| `valkey_jobs_failed_total` | counter | `component`, `type` |
| `valkey_jobs_requeued_total` | counter | `component`, `type` |
| `valkey_jobs_dead_total` | counter | `component`, `type` |
| `valkey_jobs_duration_seconds_sum` | counter | `component`, `type` |
| `valkey_jobs_duration_seconds_count` | counter | `component`, `type` |

Queued work is visible on the valkey keys directly: `jobs:ready` (score = due
ms), `jobs:inflight` (score = visibility deadline), `jobs:dead` (score =
dead-lettered-at), and a `jobs:<id>` payload hash per job. Dead-lettered jobs
can be re-enqueued manually by re-adding the id to `jobs:ready` with a due
score.

## Key layout

| Key | Type | Meaning |
| --- | --- | --- |
| `<prefix>jobs:ready` | ZSET | due score in ms; poll targets score ≤ now |
| `<prefix>jobs:inflight` | ZSET | visibility deadline in ms; reaper recovers expired |
| `<prefix>jobs:dead` | ZSET | dead-lettered at ms; inspection only |
| `<prefix>jobs:<id>` | HASH | `type`, `payload`, `max_attempts`, `created_ms`, `attempts`, `retry_start_ms`, `jitter_base`, `visibility_ms`; TTL = retention |

## Tests

Unit tests cover the Init contract, dependencies (named peers, config source),
retry math (fixed phase, jitter phase, cap, jitter range), option layering, and
claim decoding — no external service.

Integration tests (`integration_test.go`) run against a real valkey when
`VALKEY_ADDR` is set: happy-path run, scheduling with `WithDelay`/`WithRunAt`,
retry-then-dead-letter, no-handler dead-letter, visibility recovery of a hung
job, the concurrency bound, shutdown-drain requeue, and named-peer resolution.

```bash
docker run -d --rm -p 6379:6379 --name v valkey/valkey:8
VALKEY_ADDR=127.0.0.1:6379 go test -race ./...
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
