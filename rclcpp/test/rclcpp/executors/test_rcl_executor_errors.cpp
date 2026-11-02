// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>

#include "rcl_executor/executor.h"
#include "rclcpp/rclcpp.hpp"
#include "../../mocking_utils/patch.hpp"

namespace
{
// A Single/MultiThreadedExecutor that runs on the rcl_executor core.
std::unique_ptr<rclcpp::Executor> make_core_executor()
{
  const char * saved = std::getenv("ROS_EXECUTOR_CORE");
  std::string value = saved ? saved : "";
  setenv("ROS_EXECUTOR_CORE", "1", 1);
  auto executor = std::make_unique<rclcpp::executors::SingleThreadedExecutor>();
  if (saved) {setenv("ROS_EXECUTOR_CORE", value.c_str(), 1);} else {unsetenv("ROS_EXECUTOR_CORE");}
  return executor;
}

// A waitable whose readiness callback the test calls, as a publisher's notifier would.
class NotifiedWaitable : public rclcpp::Waitable
{
public:
  size_t get_number_of_ready_guard_conditions() override {return 1;}
  void add_to_wait_set(rcl_wait_set_t & wait_set) override {guard.add_to_wait_set(wait_set);}
  bool is_ready(const rcl_wait_set_t &) override {return false;}
  std::shared_ptr<void> take_data() override {return nullptr;}
  std::shared_ptr<void> take_data_by_entity_id(size_t) override {return nullptr;}
  void execute(const std::shared_ptr<void> &) override {}
  void set_on_ready_callback(std::function<void(size_t, int)> callback) override
  {
    on_ready = std::move(callback);
  }
  void clear_on_ready_callback() override {on_ready = nullptr;}
  std::function<void(size_t, int)> on_ready;
  rclcpp::GuardCondition guard;
};

class Errors : public ::testing::Test
{
protected:
  void SetUp() override {rclcpp::init(0, nullptr);}
  void TearDown() override {rclcpp::shutdown();}
};
}  // namespace

// A failed release of a claimed item is reported to the caller instead of ending the process.
TEST_F(Errors, failed_complete_throws_from_spin_once_and_spin_some)
{
  auto node = std::make_shared<rclcpp::Node>("failed_complete");
  int calls = 0;
  // The mocked complete leaves its claim held; a reentrant group lets the next call claim again.
  auto group = node->create_callback_group(rclcpp::CallbackGroupType::Reentrant);
  auto timer = node->create_wall_timer(std::chrono::milliseconds(1), [&] {++calls;}, group);
  auto executor = make_core_executor();
  executor->add_node(node);
  {
    auto mock = mocking_utils::patch_and_return(
      "lib:rclcpp", rcl_executor_complete, RCL_RET_ERROR);
    EXPECT_THROW(executor->spin_once(std::chrono::seconds(1)), rclcpp::exceptions::RCLError);
    rclcpp::sleep_for(std::chrono::milliseconds(5));
    EXPECT_THROW(executor->spin_some(std::chrono::seconds(1)), rclcpp::exceptions::RCLError);
  }
  // spin_some may first claim the executor's own maintenance item, before the timer.
  EXPECT_GE(calls, 1);
  executor->remove_node(node);
}

// A failed notify inside a readiness callback does not throw into the notifying thread.
TEST_F(Errors, failed_notify_does_not_throw_into_the_notifier)
{
  auto node = std::make_shared<rclcpp::Node>("failed_notify");
  auto waitable = std::make_shared<NotifiedWaitable>();
  node->get_node_waitables_interface()->add_waitable(waitable, nullptr);
  auto executor = make_core_executor();
  executor->add_node(node);
  executor->spin_some(std::chrono::milliseconds(10));
  ASSERT_TRUE(static_cast<bool>(waitable->on_ready));
  {
    auto mock = mocking_utils::patch_and_return(
      "lib:rclcpp", rcl_executor_notify, RCL_RET_ERROR);
    EXPECT_NO_THROW(waitable->on_ready(1, 0));
  }
  executor->remove_node(node);
  node->get_node_waitables_interface()->remove_waitable(waitable, nullptr);
}
