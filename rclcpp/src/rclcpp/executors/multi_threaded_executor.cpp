// Copyright 2015 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rclcpp/executors/multi_threaded_executor.hpp"

#include <chrono>
#include <functional>
#include <memory>
#include <vector>

#include "rcpputils/scope_exit.hpp"

#include "rclcpp/logging.hpp"
#include "rclcpp/utilities.hpp"
#include "../executor_implementation.hpp"

using rclcpp::executors::MultiThreadedExecutor;

MultiThreadedExecutor::MultiThreadedExecutor(
  const rclcpp::ExecutorOptions & options,
  size_t number_of_threads,
  bool yield_before_execute,
  std::chrono::nanoseconds next_exec_timeout)
: rclcpp::Executor(options),
  yield_before_execute_(yield_before_execute),
  next_exec_timeout_(next_exec_timeout)
{
  number_of_threads_ = number_of_threads > 0 ?
    number_of_threads :
    std::max(std::thread::hardware_concurrency(), 2U);

  if (rclcpp::executor_core_requested()) {
    impl_->core = std::make_unique<rclcpp::ExecutorCore>(*this, number_of_threads_,
      RCL_EXECUTOR_WAIT_SET);
    impl_->core->configure(yield_before_execute_, next_exec_timeout_);
  }

  if (number_of_threads_ == 1) {
    RCLCPP_WARN(
      rclcpp::get_logger("rclcpp"),
      "MultiThreadedExecutor is used with a single thread.\n"
      "Use the SingleThreadedExecutor instead.");
  }
}

MultiThreadedExecutor::~MultiThreadedExecutor() {}

void
MultiThreadedExecutor::spin()
{
  if (impl_->core) {return impl_->core->spin();}
  if (spinning.exchange(true)) {
    throw std::runtime_error("spin() called while already spinning");
  }
  RCPPUTILS_SCOPE_EXIT(wait_result_.reset();this->spinning.store(false););
  std::vector<std::thread> threads;
  size_t thread_id = 0;
  {
    std::lock_guard wait_lock{wait_mutex_};
    for (; thread_id < number_of_threads_ - 1; ++thread_id) {
      auto func = std::bind(&MultiThreadedExecutor::run, this, thread_id);
      threads.emplace_back(func);
    }
  }

  run(thread_id);
  for (auto & thread : threads) {
    thread.join();
  }
}

size_t
MultiThreadedExecutor::get_number_of_threads()
{
  return number_of_threads_;
}

bool
MultiThreadedExecutor::left_out_of_wait(const rclcpp::AnyExecutable & any_exec)
{
  std::lock_guard guard{mutex_};
  const auto & c = current_collection_;
  if (any_exec.subscription) {
    return !c.subscriptions.count(any_exec.subscription->get_subscription_handle().get());
  }
  if (any_exec.timer) {
    return !c.timers.count(any_exec.timer->get_timer_handle().get());
  }
  if (any_exec.service) {
    return !c.services.count(any_exec.service->get_service_handle().get());
  }
  if (any_exec.client) {
    return !c.clients.count(any_exec.client->get_client_handle().get());
  }
  return any_exec.waitable && !c.waitables.count(any_exec.waitable.get());
}

void
MultiThreadedExecutor::run(size_t this_thread_number)
{
  (void)this_thread_number;
  while (rclcpp::ok(this->context_) && spinning.load()) {
    rclcpp::AnyExecutable any_exec;
    {
      std::lock_guard wait_lock{wait_mutex_};
      if (!rclcpp::ok(this->context_) || !spinning.load()) {
        return;
      }
      if (!get_next_executable(any_exec, next_exec_timeout_)) {
        // Woken with nothing this thread may run: the work is in groups other threads are
        // running. Leave those out of the next wait, or it returns at once, over and over.
        if (wait_result_ && wait_result_->kind() == WaitResultKind::Ready) {
          entities_need_rebuild_.store(true);
        }
        continue;
      }
    }
    if (yield_before_execute_) {
      std::this_thread::yield();
    }

    execute_any_executable(any_exec);

    if (any_exec.callback_group &&
      any_exec.callback_group->type() == CallbackGroupType::MutuallyExclusive)
    {
      if (left_out_of_wait(any_exec)) {
        entities_need_rebuild_.store(true);
      }
      try {
        interrupt_guard_condition_->trigger();
      } catch (const rclcpp::exceptions::RCLError & ex) {
        throw std::runtime_error(
                std::string(
                  "Failed to trigger guard condition on callback group change: ") + ex.what());
      }
    }

    // Clear the callback_group to prevent the AnyExecutable destructor from
    // resetting the callback group `can_be_taken_from`
    any_exec.callback_group.reset();
  }
}
