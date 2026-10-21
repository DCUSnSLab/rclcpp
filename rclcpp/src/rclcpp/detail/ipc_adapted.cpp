// Copyright 2026 nevlife
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

#include "rclcpp/detail/ipc_adapted.hpp"

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace rclcpp
{
namespace detail
{
namespace
{
std::mutex takes_mutex;
std::unordered_map<const rcl_subscription_t *, std::shared_ptr<const IpcAdaptedTake>> takes;
// Most processes have no adapted subscription; their takes skip the lock.
std::atomic<size_t> take_count{0};
}  // namespace

void
set_ipc_adapted_take(
  const rcl_subscription_t * subscription, std::shared_ptr<const IpcAdaptedTake> take)
{
  std::lock_guard<std::mutex> lock(takes_mutex);
  if (take) {
    takes[subscription] = std::move(take);
  } else {
    takes.erase(subscription);
  }
  take_count.store(takes.size(), std::memory_order_release);
}

std::shared_ptr<const IpcAdaptedTake>
get_ipc_adapted_take(const rcl_subscription_t * subscription)
{
  if (take_count.load(std::memory_order_acquire) == 0) {
    return nullptr;
  }
  std::lock_guard<std::mutex> lock(takes_mutex);
  auto it = takes.find(subscription);
  return it == takes.end() ? nullptr : it->second;
}

}  // namespace detail
}  // namespace rclcpp
