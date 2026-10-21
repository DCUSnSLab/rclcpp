// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#ifndef RCLCPP__EXPERIMENTAL__EXECUTORS__RCL_EXECUTOR_HPP_
#define RCLCPP__EXPERIMENTAL__EXECUTORS__RCL_EXECUTOR_HPP_

#include <rcl_executor/executor.h>
#include <functional>
#include <memory>
#include <rclcpp/executor.hpp>

namespace rclcpp
{
namespace experimental
{
namespace executors
{
class RclExecutor : public rclcpp::Executor
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(RclExecutor)
  RCLCPP_PUBLIC
  explicit RclExecutor(
    const ExecutorOptions & options = ExecutorOptions(), size_t number_of_threads = 1);
  RCLCPP_PUBLIC
  RclExecutor(
    const ExecutorOptions & options, size_t number_of_threads,
    rcl_executor_selection_policy_t selection_policy);
  enum class Mode {Shared, Partitioned};
  RCLCPP_PUBLIC
  RclExecutor(
    const ExecutorOptions & options, Mode mode,
    rcl_executor_selection_policy_t selection_policy = RCL_EXECUTOR_WAIT_SET);
  // Index 0 of a shared executor is the thread that calls spin(); attributes the callback sets
  // there remain after spin() returns. Partition mode calls it only on the threads it creates.
  using ThreadStart = std::function<void(size_t, CallbackGroup::SharedPtr)>;
  RCLCPP_PUBLIC
  void on_thread_start(ThreadStart callback);
  // Notify by registration ID; remove before releasing the callback's captured resources.
  RCLCPP_PUBLIC
  rcl_executor_id_t add_source(CallbackGroup::SharedPtr group, std::function<void()> dispatch);
  RCLCPP_PUBLIC
  void notify_source(rcl_executor_id_t source, size_t count = 1);
  RCLCPP_PUBLIC
  void remove_source(rcl_executor_id_t source);
  RCLCPP_PUBLIC
  void set_group_priority(CallbackGroup::SharedPtr group, int32_t priority);
  RCLCPP_PUBLIC
  ~RclExecutor() override;
  RCLCPP_PUBLIC
  void spin() override;
  RCLCPP_PUBLIC
  void cancel() override;
  RCLCPP_PUBLIC
  void spin_some(std::chrono::nanoseconds max_duration = std::chrono::nanoseconds(0)) override;
  RCLCPP_PUBLIC
  void spin_all(std::chrono::nanoseconds max_duration) override;
  using Executor::add_node;
  using Executor::remove_node;
  RCLCPP_PUBLIC
  void add_node(node_interfaces::NodeBaseInterface::SharedPtr node, bool notify = true) override;
  RCLCPP_PUBLIC
  void remove_node(node_interfaces::NodeBaseInterface::SharedPtr node, bool notify = true) override;
  RCLCPP_PUBLIC
  void add_callback_group(
    CallbackGroup::SharedPtr group, node_interfaces::NodeBaseInterface::SharedPtr node,
    bool notify = true) override;
  RCLCPP_PUBLIC
  void remove_callback_group(CallbackGroup::SharedPtr group, bool notify = true) override;

protected:
  RCLCPP_PUBLIC
  void spin_once_impl(std::chrono::nanoseconds timeout) override;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  void drain(std::chrono::nanoseconds duration, bool exhaustive);
};
}  // namespace executors
}  // namespace experimental
}  // namespace rclcpp
#endif  // RCLCPP__EXPERIMENTAL__EXECUTORS__RCL_EXECUTOR_HPP_
