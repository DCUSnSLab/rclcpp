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

#ifndef RCLCPP__DETAIL__IPC_ADAPTED_HPP_
#define RCLCPP__DETAIL__IPC_ADAPTED_HPP_

#include <functional>
#include <memory>
#include <string>
#include <typeinfo>

#include "rcl/subscription.h"
#include "rclcpp/message_info.hpp"
#include "rclcpp/visibility_control.hpp"

namespace rclcpp
{
namespace detail
{

/// The owner a type adapted publisher gives the rcl broker; ros is filled only when read.
template<typename CustomT, typename ROSMessageT>
struct IpcAdapted
{
  std::unique_ptr<CustomT> custom;
  std::shared_ptr<ROSMessageT> ros;

  static const char * key() {return typeid(IpcAdapted).name();}
};

struct IpcAdaptedTake
{
  // A destructor compiled from an older header does not remove the entry; the owner tells it
  // apart from a later subscription that reuses the handle address.
  const void * owner;
  std::string key;
  std::function<void(const void * adapted, const std::shared_ptr<void> & holder,
    const rclcpp::MessageInfo & info)> handle;
};

/// Kept beside the subscription, whose class layout stays as it is; nullptr removes it.
RCLCPP_PUBLIC
void
set_ipc_adapted_take(
  const rcl_subscription_t * subscription, std::shared_ptr<const IpcAdaptedTake> take);

RCLCPP_PUBLIC
std::shared_ptr<const IpcAdaptedTake>
get_ipc_adapted_take(const rcl_subscription_t * subscription);

}  // namespace detail
}  // namespace rclcpp

#endif  // RCLCPP__DETAIL__IPC_ADAPTED_HPP_
