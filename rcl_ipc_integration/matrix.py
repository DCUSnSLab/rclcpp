#!/usr/bin/env python3
# Copyright 2026 nev
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise real C++ and Python nodes sharing one leased rcl context."""

import argparse
import array
import ctypes
import json
import math
import os
import statistics
import time

import _ipc_native
import rclpy
from rclpy.context import Context
from rclpy.executors import SingleThreadedExecutor
from sensor_msgs.msg import Image


def main():
    """Check payloads and measure all typed language pairs through ordinary APIs."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--pair', choices=['cc', 'cp', 'pc', 'pp'], required=True)
    parser.add_argument('--bytes', type=int, required=True)
    parser.add_argument('--samples', type=int, default=80)
    parser.add_argument('--legacy', action='store_true')
    parser.add_argument('--functional', action='store_true')
    args = parser.parse_args()
    start_load = os.getloadavg()[0]
    if not args.functional:
        assert start_load < 2, start_load
    topic = '/ipc_matrix'
    native = _ipc_native.Container(args.pair[0] == 'c', args.pair[1] == 'c', args.legacy, topic)
    context = Context()
    context.init(context_lease=native.lease(), initialize_logging=False)
    node = rclpy.create_node('python_component', context=context,
                             start_parameter_services=False, enable_rosout=False)
    executor = SingleThreadedExecutor(context=context)
    executor.add_node(node)
    seen = []
    latencies = []
    retained = []

    def callback(message):
        elapsed = time.monotonic_ns() - (
            message.header.stamp.sec * 10**9 + message.header.stamp.nanosec)
        assert len(message.data) == args.bytes
        assert message.data.count(42) == args.bytes
        seen.append(message.height)
        latencies.append(elapsed / 1000)
        if message.height == 9:
            retained.append(message)

    if args.pair[1] == 'p':
        node.create_subscription(Image, topic, callback, 32)
    if args.pair[0] == 'p':
        publisher = node.create_publisher(Image, topic, 32)
        payload = array.array('B', [42]) * args.bytes
    deadline = time.monotonic() + 5
    while (publisher.get_subscription_count() if args.pair[0] == 'p'
           else native.matched()) < 1:
        assert time.monotonic() < deadline, 'discovery timeout'
        time.sleep(.01)
    if args.pair != 'pp':
        expected = os.environ.get('RCL_INTRA_PROCESS') == '1' and (
            os.environ.get('RMW_IMPLEMENTATION') == 'rmw_zenoh_cpp')
        assert native.enabled() == expected, (native.enabled(), expected)
    counter = ctypes.CDLL(None)
    reset = counter.memcount_reset
    cpp_latencies = []
    for sequence in range(10 + args.samples):
        if sequence == 10:
            reset()
            latencies.clear()
            cpp_latencies.clear()
        counter.memcount_pause()
        if args.pair[0] == 'p':
            message = Image()
            message.data = payload
            message.height = sequence
            message.width = args.bytes
            counter.memcount_resume()
            start = time.monotonic_ns()
            message.header.stamp.sec = start // 10**9
            message.header.stamp.nanosec = start % 10**9
            publisher.publish(message)
        else:
            counter.memcount_resume()
            native.publish(args.bytes, sequence)
        if args.pair[1] == 'c':
            cpp_latencies.append(native.wait(sequence) / 1000)
        else:
            deadline = time.monotonic() + 3
            while not seen or seen[-1] != sequence:
                assert time.monotonic() < deadline, (sequence, seen[-5:])
                executor.spin_once(timeout_sec=.05)
    counter.memcount_pause()
    if args.pair[1] == 'p':
        assert seen == list(range(10 + args.samples)), seen
        assert retained[0].height == 9 and retained[0].data.count(42) == args.bytes
    else:
        assert native.count() == 10 + args.samples
    ctypes.CDLL(None).memcount_stop()
    values = latencies if args.pair[1] == 'p' else cpp_latencies
    end_load = os.getloadavg()[0]
    result = {
        'pair': args.pair, 'bytes': args.bytes, 'samples': args.samples,
        'route': 'legacy' if args.legacy else os.environ.get('RCL_INTRA_PROCESS', '0'),
        'executor': os.environ.get('ROS_EXECUTOR_CORE', '1'),
        'load_start': start_load, 'load_end': end_load,
        'p50_us': None if args.functional else statistics.median(values),
        'p99_us': None if args.functional else sorted(values)[math.ceil(len(values) * .99) - 1]}
    executor.shutdown()
    executor.remove_node(node)
    node.destroy_node()
    context.destroy()
    native.close()
    if not args.functional:
        assert end_load < 2, end_load
    print(json.dumps(result))


if __name__ == '__main__':
    main()
