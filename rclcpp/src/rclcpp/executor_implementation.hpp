// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#ifndef RCLCPP__EXECUTOR_IMPLEMENTATION_HPP_
#define RCLCPP__EXECUTOR_IMPLEMENTATION_HPP_

#include <cstdlib>
#include <cstring>
#include <memory>

#include "rclcpp/experimental/executors/rcl_executor.hpp"

namespace rclcpp
{
class ExecutorCore
{
public:
  using ThreadStart = experimental::executors::RclExecutor::ThreadStart;
  ExecutorCore(
    Executor & owner, size_t threads, rcl_executor_selection_policy_t policy,
    bool partitioned = false);
  ~ExecutorCore();
  void sync();
  void spin();
  void cancel();
  void spin_once(std::chrono::nanoseconds timeout);
  void end_round();
  void drain(std::chrono::nanoseconds duration, bool exhaustive);
  void on_thread_start(ThreadStart callback);
  void set_group_priority(CallbackGroup::SharedPtr group, int32_t priority);
  rcl_executor_id_t add_source(CallbackGroup::SharedPtr group, std::function<void()> dispatch);
  void notify_source(rcl_executor_id_t source, size_t count);
  void remove_source(rcl_executor_id_t source);
  void configure(bool yield_before_execute, std::chrono::nanoseconds timeout);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

inline bool executor_core_requested()
{
  const char * value = std::getenv("ROS_EXECUTOR_CORE");
  return value && std::strcmp(value, "1") == 0;
}

class ExecutorImplementation
{
public:
  std::unique_ptr<ExecutorCore> core;
};
}  // namespace rclcpp
#endif  // RCLCPP__EXECUTOR_IMPLEMENTATION_HPP_
