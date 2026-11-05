# Configuration

Each machine reads `~/.config/potluck/node.toml`, or `$POTLUCK_CONFIG`; the daemon reads it when it
starts. The file is optional, and unknown keys are an error.

| Key | Default | Meaning |
| --- | --- | --- |
| `name` | the short hostname | this machine's name in the cluster, kept from its daemon's first start |
| `port` | `7465` | the port members reach this machine on |
| `listen` | every IPv4 interface | the IP address to listen on, such as `127.0.0.1` or `::` |
| `address` | the hostname and `port` | how members reach this machine: `host:port`, or a host to use with `port` |
| `ram_gib` | total RAM minus 4 GiB or 10%, whichever is more | what RAM reservations may add up to, more than 0 and at most 1048576 |
| `workspace_budget_percent` | `10` | the share of the disk workspaces may use, 0 to 100 |
| `workspace_budget_min_gib` | `50` | the smallest workspace budget, in GiB |
| `kill_grace_s` | `10` | seconds from SIGTERM to SIGKILL, at most 86400 |
| `stall_minutes` | `10` | under 1% of a CPU core and no output this long is a stall, at most 10080; `0` turns stall checks off |
| `retention_days` | `14` | how long job records, output logs and kept files are kept after the job ends, counted from when this machine learned of the end, on its own clock |
| `log_max_mib` | `1024` | how much of a job's output its log keeps before it drops the rest but the last 16 MiB (see [Output](jobs.md#output)); `0`: no limit |

Each `[[gpu]]` entry names one of the machine's GPUs:

```toml
[[gpu]]
match = "nvidia"              # a GPU selector that names exactly one GPU here
alias = "5080"                # a name for it in --gpu and --on gpu=
env = { MY_GPU = "5080" }     # set in jobs that hold it
```
