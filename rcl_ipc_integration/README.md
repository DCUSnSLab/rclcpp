# Typed intra-process integration

This package embeds real C++ and Python nodes using a shared rcl context. It tests
the opt-in context broker, the ordinary RMW path, and the existing C++ IPM path.
Build `rmw`, `zenoh_cpp_vendor`, `rmw_zenoh_cpp`, `rcl`, `rclcpp`, `rclpy`, and
this package into an overlay. Build `zenoh_cpp_vendor` from the same `rmw_zenoh`
fork as `rmw_zenoh_cpp` (zenoh-c 1.10.1): the fork's default configuration uses
zenoh 1.10 keys, so the Jazzy binary vendor (zenoh-c 1.8.0) cannot be reused.
Use the router from the same overlay.

After sourcing the overlay, add its `lib/rcl_ipc_integration` directory to
`PYTHONPATH`. Set `RMW_IMPLEMENTATION=rmw_zenoh_cpp` and run a router on a port
that no other session uses. Enable the broker before initializing any context with
`RCL_INTRA_PROCESS=1` (default off).

`Container.lease()` returns a capsule named `rcl_context_shared_ptr` holding a
shared pointer to its actual rcl context. Python calls
`Context.init(context_lease=container.lease(), initialize_logging=False)` and creates
ordinary rclpy nodes in that context. The container owns shutdown. Python nodes
and executors must be destroyed before closing the container. The capsule is a
C++ runtime ABI contract; it is not a raw pointer transfer.

For broker delivery use default C++ endpoint settings. Explicitly selecting
rclcpp's existing intra-process option selects its legacy manager. Other RMWs
without the optional routing extension always retain ordinary RMW behavior.

Functional checks:

```bash
python3 test_context_lease.py
python3 test_remote.py
```

`test_remote.py` launches a second process and checks that a typed broker reader,
a raw reader, and a remote reader each receive local and remote publications once.
Set `ROS_EXECUTOR_CORE=0` for classic executors or `1` for the common executor.

`matrix.py` accepts `--pair cc|cp|pc|pp`, `--bytes`, `--samples`, and `--legacy`
(the last only for C++/C++). It checks every payload/sequence. It needs a
payload-copy counter preloaded with `LD_PRELOAD` that exports `memcount_reset`,
`memcount_pause`, `memcount_resume` and `memcount_stop`; that library is not part
of this package. Without `--functional` the script asserts that the one-minute
system load is below 2 at start and end; `--functional` checks delivery and never
reports timing. Timing includes generated language conversions and executor wakeup,
starting immediately before publish and ending at callback entry. Payload setup
and validation are excluded from the latency interval. Copy counters exclude
payload setup, warmup and teardown.
